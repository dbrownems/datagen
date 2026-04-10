using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AnalysisServices.AdomdClient;

namespace Datagen
{
    public class QueryResult
    {
        public int UserIndex { get; set; }
        public int QueryIndex { get; set; }
        public int Iteration { get; set; }
        public int RowCount { get; set; }
        public double DurationMs { get; set; }
        public double StartTimeMs { get; set; }
        public string? Error { get; set; }
    }

    public static class QueryRunner
    {
        public static string RunLoadTest(
            string[] queries, string xmlaEndpoint, string dataset, string token,
            string[] userEmails, string[] userRoles,
            int durationSeconds = 60, int queriesPerBatch = 4,
            int pauseBetweenIterationsMs = 1000, int pauseBetweenQueriesMs = 0)
        {
            var allResults = new ConcurrentBag<QueryResult>();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            var testStart = Stopwatch.StartNew();

            Console.WriteLine($"[QueryRunner] Starting: {userEmails.Length} users, {queries.Length} queries, {durationSeconds}s, {queriesPerBatch} concurrent/user, pause={pauseBetweenIterationsMs}ms/iter, {pauseBetweenQueriesMs}ms/query");

            var userTasks = new Task[userEmails.Length];
            for (int u = 0; u < userEmails.Length; u++)
            {
                int userIdx = u;
                userTasks[u] = Task.Run(() =>
                    SimulateUser(userIdx, queries, xmlaEndpoint, dataset, token,
                        userEmails[userIdx], userRoles[userIdx],
                        queriesPerBatch, pauseBetweenIterationsMs, pauseBetweenQueriesMs,
                        allResults, testStart, cts.Token));
            }

            Task.WaitAll(userTasks);
            testStart.Stop();
            Console.WriteLine($"[QueryRunner] Done: {allResults.Count} executions in {testStart.Elapsed.TotalSeconds:F1}s");

            return BuildStats(allResults.ToList(), testStart.Elapsed.TotalMilliseconds,
                userEmails.Length, queries.Length);
        }

        private static void SimulateUser(
            int userIndex, string[] queries, string xmlaEndpoint, string dataset,
            string token, string email, string role, int queriesPerBatch, int pauseMs,
            int pauseBetweenQueriesMs,
            ConcurrentBag<QueryResult> results, Stopwatch testStart, CancellationToken ct)
        {
            string connStr =
                $"Data Source={xmlaEndpoint};Initial Catalog={dataset};" +
                $"password={token};Timeout=7200;CustomData={email};Roles={role};";

            Console.WriteLine($"[User {userIndex}] Connecting ({email}) ...");
            var connections = new AdomdConnection[queriesPerBatch];
            for (int c = 0; c < queriesPerBatch; c++)
            {
                connections[c] = new AdomdConnection(connStr);
                connections[c].Open();
            }
            Console.WriteLine($"[User {userIndex}] {queriesPerBatch} connections open");

            try
            {
                int iteration = 0;
                while (!ct.IsCancellationRequested)
                {
                    iteration++;
                    RunIteration(userIndex, iteration, queries, connections,
                        pauseBetweenQueriesMs, results, testStart, ct);
                    if (ct.IsCancellationRequested) break;
                    try { Task.Delay(pauseMs, ct).Wait(ct); }
                    catch (OperationCanceledException) { break; }
                }
                Console.WriteLine($"[User {userIndex}] Done after {iteration} iterations");
            }
            finally
            {
                for (int c = 0; c < connections.Length; c++)
                    if (connections[c] != null) { connections[c].Close(); connections[c].Dispose(); }
            }
        }

        private static void RunIteration(
            int userIndex, int iteration, string[] queries,
            AdomdConnection[] connections, int pauseBetweenQueriesMs,
            ConcurrentBag<QueryResult> results, Stopwatch testStart,
            CancellationToken ct)
        {
            int max = connections.Length;
            using var sem = new SemaphoreSlim(max);
            var connSlots = new ConcurrentQueue<AdomdConnection>(connections);
            var tasks = new List<Task>();

            for (int q = 0; q < queries.Length; q++)
            {
                if (ct.IsCancellationRequested) break;
                try { sem.Wait(ct); }
                catch (OperationCanceledException) { break; }

                int qi = q; int iter = iteration;
                tasks.Add(Task.Run(() =>
                {
                    AdomdConnection? conn = null;
                    try
                    {
                        if (!connSlots.TryDequeue(out conn))
                            throw new InvalidOperationException("No connection available");

                        var r = ExecuteQuery(userIndex, qi, iter, queries[qi], conn, testStart);
                        results.Add(r);
                        if (r.Error != null)
                        {
                            Console.WriteLine($"[User {userIndex}] Q{qi} iter {iter} FAILED: {r.Error}");
                            throw new Exception(r.Error);
                        }
                        if (pauseBetweenQueriesMs > 0)
                        {
                            try { Task.Delay(pauseBetweenQueriesMs, ct).Wait(ct); }
                            catch (OperationCanceledException) { }
                        }
                    }
                    finally
                    {
                        if (conn != null) connSlots.Enqueue(conn);
                        sem.Release();
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray());
        }

        private static QueryResult ExecuteQuery(int userIndex, int queryIndex,
            int iteration, string query, AdomdConnection conn, Stopwatch testStart)
        {
            var result = new QueryResult {
                UserIndex = userIndex, QueryIndex = queryIndex, Iteration = iteration,
                StartTimeMs = Math.Round(testStart.Elapsed.TotalMilliseconds) };
            try
            {
                var cmd = new AdomdCommand(query, conn);
                var sw = Stopwatch.StartNew();
                using var reader = cmd.ExecuteReader();
                int count = 0;
                while (reader.Read()) count++;
                sw.Stop();
                result.RowCount = count;
                result.DurationMs = sw.Elapsed.TotalMilliseconds;
            }
            catch (Exception ex)
            {
                result.Error = ex.Message.Length > 500 ? ex.Message[..500] : ex.Message;
            }
            return result;
        }

        private static string BuildStats(List<QueryResult> results, double totalMs,
            int nUsers, int nQueries)
        {
            var ok = results.Where(r => r.Error == null).ToList();
            var fail = results.Where(r => r.Error != null).ToList();
            var durs = ok.Select(r => r.DurationMs).OrderBy(d => d).ToList();
            int maxIter = results.Any() ? results.Max(r => r.Iteration) : 0;

            var stats = new Dictionary<string, object>
            {
                ["totalDurationMs"] = Math.Round(totalMs),
                ["users"] = nUsers,
                ["queriesPerIteration"] = nQueries,
                ["totalExecutions"] = results.Count,
                ["successfulExecutions"] = ok.Count,
                ["failedExecutions"] = fail.Count,
                ["maxIteration"] = maxIter,
                ["qps"] = Math.Round(ok.Count / (totalMs / 1000), 1),
            };

            if (durs.Any())
                stats["latency"] = new Dictionary<string, object>
                {
                    ["min"] = Math.Round(durs.First()),
                    ["max"] = Math.Round(durs.Last()),
                    ["mean"] = Math.Round(durs.Average()),
                    ["median"] = Math.Round(Pct(durs, 50)),
                    ["p95"] = Math.Round(Pct(durs, 95)),
                    ["p99"] = Math.Round(Pct(durs, 99)),
                };

            var perUser = results.GroupBy(r => r.UserIndex).OrderBy(g => g.Key)
                .Select(g => new Dictionary<string, object>
                {
                    ["userIndex"] = g.Key,
                    ["iterations"] = g.Max(r => r.Iteration),
                    ["executions"] = g.Count(),
                    ["errors"] = g.Count(r => r.Error != null),
                    ["meanLatencyMs"] = g.Where(r => r.Error == null).Select(r => r.DurationMs)
                        .DefaultIfEmpty(0).Average() is var avg ? Math.Round(avg) : 0,
                }).ToList();
            stats["perUser"] = perUser;

            if (fail.Any())
                stats["sampleErrors"] = fail.Take(5)
                    .Select(r => new { r.UserIndex, r.QueryIndex, r.Iteration, r.Error }).ToList();

            // Time-series: per-second buckets for glitch detection
            var timeline = ok.GroupBy(r => (int)(r.StartTimeMs / 1000))
                .OrderBy(g => g.Key)
                .Select(g =>
                {
                    var d = g.Select(r => r.DurationMs).OrderBy(x => x).ToList();
                    return new Dictionary<string, object>
                    {
                        ["second"] = g.Key,
                        ["count"] = g.Count(),
                        ["meanMs"] = Math.Round(d.Average()),
                        ["p50Ms"] = Math.Round(Pct(d, 50)),
                        ["p95Ms"] = Math.Round(Pct(d, 95)),
                        ["maxMs"] = Math.Round(d.Last()),
                    };
                }).ToList();
            stats["timeline"] = timeline;

            // Raw executions sorted by start time (for detailed analysis)
            stats["executions"] = ok.OrderBy(r => r.StartTimeMs)
                .Select(r => new Dictionary<string, object>
                {
                    ["t"] = r.StartTimeMs,
                    ["ms"] = Math.Round(r.DurationMs),
                    ["u"] = r.UserIndex,
                    ["q"] = r.QueryIndex,
                }).ToList();

            return JsonSerializer.Serialize(stats, new JsonSerializerOptions { WriteIndented = true });
        }

        private static double Pct(List<double> s, double p) =>
            !s.Any() ? 0 : s[Math.Clamp((int)Math.Ceiling(p / 100.0 * s.Count) - 1, 0, s.Count - 1)];
    }
}
