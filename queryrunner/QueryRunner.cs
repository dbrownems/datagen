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
        public string? Error { get; set; }
    }

    public static class QueryRunner
    {
        // Collect errors that happen outside individual query execution
        private static readonly ConcurrentBag<string> _infraErrors = new();

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
            _infraErrors.Clear();
            var allResults = new ConcurrentBag<QueryResult>();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            var testStart = Stopwatch.StartNew();

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

            // Wait for all user tasks — they exit on cancellation or error
            try { Task.WaitAll(userTasks); }
            catch (AggregateException ae)
            {
                foreach (var ex in ae.Flatten().InnerExceptions)
                {
                    if (ex is not OperationCanceledException)
                        _infraErrors.Add($"User task failed: {ex.Message}");
                }
            }
            testStart.Stop();

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

            var connections = new AdomdConnection[queriesPerBatch];
            try
            {
                for (int c = 0; c < queriesPerBatch; c++)
                {
                    connections[c] = new AdomdConnection(connStr);
                    connections[c].Open();
                }
            }
            catch (Exception ex)
            {
                _infraErrors.Add($"User {userIndex} connection failed: {ex.Message}");
                return;
            }

            try
            {
                int iteration = 0;
                while (!ct.IsCancellationRequested)
                {
                    iteration++;
                    RunIteration(userIndex, iteration, queries, connections,
                        results, ct);

                    if (ct.IsCancellationRequested) break;

                    // Pause — OperationCanceledException is expected at test end
                    try { Task.Delay(pauseMs, ct).Wait(ct); }
                    catch (OperationCanceledException) { break; }
                }
            }
            finally
            {
                for (int c = 0; c < connections.Length; c++)
                {
                    if (connections[c] == null) continue;
                    try { connections[c].Close(); connections[c].Dispose(); }
                    catch (Exception ex)
                    {
                        _infraErrors.Add($"User {userIndex} conn {c} close failed: {ex.Message}");
                    }
                }
            }
        }

        private static void RunIteration(
            int userIndex, int iteration, string[] queries,
            AdomdConnection[] connections,
            ConcurrentBag<QueryResult> results,
            CancellationToken ct)
        {
            int maxConcurrent = connections.Length;
            using var semaphore = new SemaphoreSlim(maxConcurrent);
            var connSlots = new ConcurrentQueue<AdomdConnection>(connections);
            var tasks = new List<Task>();

            for (int q = 0; q < queries.Length; q++)
            {
                if (ct.IsCancellationRequested) break;

                // Semaphore wait — OperationCanceledException is expected at test end
                try { semaphore.Wait(ct); }
                catch (OperationCanceledException) { break; }

                int queryIdx = q;
                int iter = iteration;

                tasks.Add(Task.Run(() =>
                {
                    AdomdConnection? conn = null;
                    try
                    {
                        if (!connSlots.TryDequeue(out conn))
                        {
                            results.Add(new QueryResult
                            {
                                UserIndex = userIndex, QueryIndex = queryIdx,
                                Iteration = iter, Error = "No connection available",
                            });
                            return;
                        }
                        var result = ExecuteQuery(userIndex, queryIdx, iter,
                            queries[queryIdx], conn);
                        results.Add(result);
                    }
                    finally
                    {
                        if (conn != null) connSlots.Enqueue(conn);
                        semaphore.Release();
                    }
                }));
            }

            // Wait for in-flight queries to complete
            try { Task.WaitAll(tasks.ToArray()); }
            catch (AggregateException ae)
            {
                foreach (var ex in ae.Flatten().InnerExceptions)
                {
                    _infraErrors.Add($"User {userIndex} iter {iteration}: {ex.Message}");
                }
            }
        }

        private static QueryResult ExecuteQuery(
            int userIndex, int queryIndex, int iteration,
            string query, AdomdConnection conn)
        {
            var result = new QueryResult
            {
                UserIndex = userIndex,
                QueryIndex = queryIndex,
                Iteration = iteration,
            };

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
                result.Error = ex.Message.Length > 300
                    ? ex.Message.Substring(0, 300)
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

            if (failed.Any())
            {
                stats["sampleErrors"] = failed.Take(5)
                    .Select(r => new { r.UserIndex, r.QueryIndex, r.Iteration, r.Error })
                    .ToList();
            }

            if (_infraErrors.Any())
            {
                stats["infrastructureErrors"] = _infraErrors.Take(10).ToList();
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
