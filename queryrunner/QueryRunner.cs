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
    /// <summary>
    /// Result of a single query execution.
    /// </summary>
    public class QueryResult
    {
        public int UserIndex { get; set; }
        public int QueryIndex { get; set; }
        public int Iteration { get; set; }
        public int RowCount { get; set; }
        public double DurationMs { get; set; }
        public string? Error { get; set; }
    }

    /// <summary>
    /// Simulates Power BI report users running DAX queries.
    /// Each user runs all queries in iterations, 4 queries at a time,
    /// with a pause between iterations.
    /// </summary>
    public static class QueryRunner
    {
        /// <summary>
        /// Run a load test simulating Power BI users.
        /// Each user gets a thread that loops:
        ///   1. Run all queries (4 concurrent)
        ///   2. Pause
        ///   3. Repeat until duration expires
        /// Returns JSON with execution statistics.
        /// </summary>
        /// <param name="queries">DAX query texts</param>
        /// <param name="xmlaEndpoint">XMLA endpoint</param>
        /// <param name="dataset">Semantic model name</param>
        /// <param name="token">Access token</param>
        /// <param name="userEmails">User emails for CustomData</param>
        /// <param name="userRoles">Role per user</param>
        /// <param name="durationSeconds">Total test duration</param>
        /// <param name="queriesPerBatch">Concurrent queries per user (default 4)</param>
        /// <param name="pauseBetweenIterationsMs">Pause between iterations in ms (default 1000)</param>
        public static string RunLoadTest(
            string[] queries,
            string xmlaEndpoint,
            string dataset,
            string token,
            string[] userEmails,
            string[] userRoles,
            int durationSeconds = 60,
            int queriesPerBatch = 4,
            int pauseBetweenIterationsMs = 1000)
        {
            var allResults = new ConcurrentBag<QueryResult>();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            var testStart = Stopwatch.StartNew();

            // One task per user
            var userTasks = new Task[userEmails.Length];
            for (int u = 0; u < userEmails.Length; u++)
            {
                int userIdx = u;
                userTasks[u] = Task.Run(() =>
                    SimulateUser(userIdx, queries, xmlaEndpoint, dataset, token,
                        userEmails[userIdx], userRoles[userIdx],
                        queriesPerBatch, pauseBetweenIterationsMs,
                        allResults, cts.Token));
            }

            try { Task.WaitAll(userTasks); }
            catch (AggregateException) { }
            testStart.Stop();

            // Build statistics
            return BuildStats(allResults.ToList(), testStart.Elapsed.TotalMilliseconds,
                userEmails.Length, queries.Length);
        }

        private static void SimulateUser(
            int userIndex, string[] queries,
            string xmlaEndpoint, string dataset, string token,
            string email, string role,
            int queriesPerBatch, int pauseMs,
            ConcurrentBag<QueryResult> results,
            CancellationToken ct)
        {
            string connStr =
                $"Data Source={xmlaEndpoint};Initial Catalog={dataset};" +
                $"password={token};Timeout=7200;" +
                $"CustomData={email};Roles={role};";

            int iteration = 0;

            while (!ct.IsCancellationRequested)
            {
                iteration++;

                // Run all queries, queriesPerBatch at a time
                RunIteration(userIndex, iteration, queries, connStr,
                    queriesPerBatch, results, ct);

                if (ct.IsCancellationRequested) break;

                // Pause between iterations (simulates user think time)
                try { Task.Delay(pauseMs, ct).Wait(ct); }
                catch (OperationCanceledException) { break; }
            }
        }

        private static void RunIteration(
            int userIndex, int iteration, string[] queries, string connStr,
            int maxConcurrent, ConcurrentBag<QueryResult> results,
            CancellationToken ct)
        {
            using var semaphore = new SemaphoreSlim(maxConcurrent);
            var tasks = new List<Task>();

            for (int q = 0; q < queries.Length; q++)
            {
                if (ct.IsCancellationRequested) break;

                try { semaphore.Wait(ct); }
                catch (OperationCanceledException) { break; }

                int queryIdx = q;
                int iter = iteration;

                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        var result = ExecuteQuery(userIndex, queryIdx, iter,
                            queries[queryIdx], connStr);
                        results.Add(result);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            try { Task.WaitAll(tasks.ToArray()); }
            catch (AggregateException) { }
        }

        private static QueryResult ExecuteQuery(
            int userIndex, int queryIndex, int iteration,
            string query, string connStr)
        {
            var result = new QueryResult
            {
                UserIndex = userIndex,
                QueryIndex = queryIndex,
                Iteration = iteration,
            };

            try
            {
                using var conn = new AdomdConnection(connStr);
                conn.Open();
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
                result.Error = ex.Message.Length > 200
                    ? ex.Message.Substring(0, 200)
                    : ex.Message;
            }

            return result;
        }

        private static string BuildStats(
            List<QueryResult> results, double totalMs,
            int nUsers, int nQueries)
        {
            var successful = results.Where(r => r.Error == null).ToList();
            var failed = results.Where(r => r.Error != null).ToList();
            var durations = successful.Select(r => r.DurationMs).OrderBy(d => d).ToList();

            int maxIteration = results.Any() ? results.Max(r => r.Iteration) : 0;

            var stats = new Dictionary<string, object>
            {
                ["totalDurationMs"] = Math.Round(totalMs),
                ["users"] = nUsers,
                ["queriesPerIteration"] = nQueries,
                ["totalExecutions"] = results.Count,
                ["successfulExecutions"] = successful.Count,
                ["failedExecutions"] = failed.Count,
                ["maxIteration"] = maxIteration,
                ["qps"] = Math.Round(successful.Count / (totalMs / 1000), 1),
            };

            if (durations.Any())
            {
                stats["latency"] = new Dictionary<string, object>
                {
                    ["min"] = Math.Round(durations.First()),
                    ["max"] = Math.Round(durations.Last()),
                    ["mean"] = Math.Round(durations.Average()),
                    ["median"] = Math.Round(Percentile(durations, 50)),
                    ["p95"] = Math.Round(Percentile(durations, 95)),
                    ["p99"] = Math.Round(Percentile(durations, 99)),
                };
            }

            // Per-user stats
            var perUser = new List<Dictionary<string, object>>();
            foreach (var g in results.GroupBy(r => r.UserIndex).OrderBy(g => g.Key))
            {
                var uSuccess = g.Where(r => r.Error == null).ToList();
                var uDurations = uSuccess.Select(r => r.DurationMs).OrderBy(d => d).ToList();
                perUser.Add(new Dictionary<string, object>
                {
                    ["userIndex"] = g.Key,
                    ["iterations"] = g.Any() ? g.Max(r => r.Iteration) : 0,
                    ["executions"] = g.Count(),
                    ["errors"] = g.Count(r => r.Error != null),
                    ["meanLatencyMs"] = uDurations.Any() ? Math.Round(uDurations.Average()) : 0,
                });
            }
            stats["perUser"] = perUser;

            // Sample errors
            if (failed.Any())
            {
                stats["sampleErrors"] = failed.Take(3)
                    .Select(r => new { r.UserIndex, r.QueryIndex, r.Error })
                    .ToList();
            }

            return JsonSerializer.Serialize(stats, new JsonSerializerOptions
            {
                WriteIndented = true,
            });
        }

        private static double Percentile(List<double> sorted, double p)
        {
            if (!sorted.Any()) return 0;
            int idx = (int)Math.Ceiling(p / 100.0 * sorted.Count) - 1;
            return sorted[Math.Max(0, Math.Min(idx, sorted.Count - 1))];
        }
    }
}
