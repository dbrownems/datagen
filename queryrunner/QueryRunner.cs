using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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

    internal class TelemetryRecord
    {
        public int QueryNumber { get; set; }
        public string UserEmail { get; set; } = "";
        public DateTime Timestamp { get; set; }
        public double StartTimeMs { get; set; }
        public double DurationMs { get; set; }
        public string Outcome { get; set; } = "";
        public string MessageText { get; set; } = "";
        public int ActiveUsers { get; set; }
    }

    public static class QueryRunner
    {

        public static string RunLoadTest(
            string[] queries, string xmlaEndpoint, string dataset, string token,
            string[] userEmails, string[] userRoles,
            int durationSeconds = 60, int queriesPerBatch = 4,
            int pauseBetweenIterationsMs = 1000, int pauseBetweenQueriesMs = 0,
            string? logDirectory = null, int userRampTimeSec = 0,
            string? logFileName = null)
        {
            var allResults = new ConcurrentBag<QueryResult>();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            var testStart = Stopwatch.StartNew();
            var testStartTime = DateTime.UtcNow;

            Console.WriteLine($"[QueryRunner] Starting: {userEmails.Length} users, {queries.Length} queries, {durationSeconds}s, {queriesPerBatch} concurrent/user, pause={pauseBetweenIterationsMs}ms/iter, {pauseBetweenQueriesMs}ms/query, ramp={userRampTimeSec}s");

            // Set up telemetry log writer
            BlockingCollection<TelemetryRecord>? telemetryQueue = null;
            Task? logWriterTask = null;
            string? logFilePath = null;

            if (!string.IsNullOrEmpty(logDirectory))
            {
                Directory.CreateDirectory(logDirectory);
                var fileName = !string.IsNullOrEmpty(logFileName)
                    ? logFileName
                    : $"LoadTest.{testStartTime:yyyyMMdd-HHmmss}.csv";
                logFilePath = Path.Combine(logDirectory, fileName);
                Console.WriteLine($"[QueryRunner] Logging to: {logFilePath}");

                // Write CSV header
                File.WriteAllText(logFilePath, "QueryNumber,UserEmail,Timestamp,StartTimeMs,DurationMs,Outcome,MessageText,ActiveUsers\n");

                telemetryQueue = new BlockingCollection<TelemetryRecord>(boundedCapacity: 10000);
                logWriterTask = Task.Run(() => LogWriterLoop(telemetryQueue, logFilePath, cts.Token));
            }

            // Shared active user counter (array so it can be captured in lambdas)
            var activeUsers = new int[] { 0 };

            // Calculate per-user start delays for ramp-up
            int nUsers = userEmails.Length;
            double rampIntervalMs = nUsers > 1 && userRampTimeSec > 0
                ? (userRampTimeSec * 1000.0) / (nUsers - 1)
                : 0;

            var totalConnectTimeMs = new long[] { 0 };
            int progressStep = Math.Max(1, nUsers / 10);
            int batchSize = 10;
            var connStrings = new string[nUsers];

            var userTasks = new Task[nUsers];
            for (int batchStart = 0; batchStart < nUsers; batchStart += batchSize)
            {
                int batchEnd = Math.Min(batchStart + batchSize, nUsers);
                var batchSw = Stopwatch.StartNew();

                // Open connections for this batch in parallel
                var connectTasks = new Task<AdomdConnection[]>[batchEnd - batchStart];
                for (int b = 0; b < connectTasks.Length; b++)
                {
                    int userIdx = batchStart + b;
                    string connStr =
                        $"Data Source={xmlaEndpoint};Initial Catalog={dataset};" +
                        $"password={token};Timeout=7200;Connect Timeout=300;" +
                        $"CustomData={userEmails[userIdx]};Roles={userRoles[userIdx]};";
                    connStrings[userIdx] = connStr;
                    connectTasks[b] = Task.Run(() =>
                    {
                        var sw = Stopwatch.StartNew();
                        var conns = new AdomdConnection[queriesPerBatch];
                        for (int c = 0; c < queriesPerBatch; c++)
                        {
                            conns[c] = new AdomdConnection(connStr);
                            conns[c].Open();
                        }
                        sw.Stop();
                        Interlocked.Add(ref totalConnectTimeMs[0], (long)sw.Elapsed.TotalMilliseconds);
                        Interlocked.Increment(ref activeUsers[0]);
                        return conns;
                    });
                }
                Task.WaitAll(connectTasks);
                batchSw.Stop();

                // Start query loops for this batch
                for (int b = 0; b < connectTasks.Length; b++)
                {
                    int userIdx = batchStart + b;
                    var connections = connectTasks[b].Result;
                    userTasks[userIdx] = Task.Run(() =>
                        SimulateUserWithConnections(userIdx, queries, userEmails[userIdx],
                            queriesPerBatch, pauseBetweenIterationsMs, pauseBetweenQueriesMs,
                            connections, connStrings[userIdx], allResults, testStart, telemetryQueue, activeUsers, cts.Token));
                }

                // Progress every 10%
                if (batchEnd % progressStep == 0 || batchEnd == nUsers)
                {
                    int active = Volatile.Read(ref activeUsers[0]);
                    double avgMs = active > 0 ? Volatile.Read(ref totalConnectTimeMs[0]) / (double)active : 0;
                    Console.WriteLine($"[QueryRunner] Ramp: {batchEnd}/{nUsers} connected, avg connect {avgMs:F0}ms, t={testStart.Elapsed.TotalSeconds:F0}s");
                }

                // Stagger: wait before next batch
                if (rampIntervalMs > 0 && batchEnd < nUsers)
                {
                    int batchDelayMs = Math.Max(0, (int)(rampIntervalMs * batchSize) - (int)batchSw.Elapsed.TotalMilliseconds);
                    if (batchDelayMs > 0)
                    {
                        try { Task.Delay(batchDelayMs, cts.Token).Wait(cts.Token); }
                        catch (OperationCanceledException) { break; }
                    }
                }
            }

            Task.WaitAll(userTasks);
            testStart.Stop();

            // Shut down log writer
            if (telemetryQueue != null)
            {
                telemetryQueue.CompleteAdding();
                logWriterTask?.Wait(TimeSpan.FromSeconds(10));
                Console.WriteLine($"[QueryRunner] Log written: {logFilePath}");
            }

            Console.WriteLine($"[QueryRunner] Done: {allResults.Count} executions in {testStart.Elapsed.TotalSeconds:F1}s");

            return BuildStats(allResults.ToList(), testStart.Elapsed.TotalMilliseconds,
                userEmails.Length, queries.Length, logFilePath);
        }

        private static void LogWriterLoop(BlockingCollection<TelemetryRecord> queue,
            string logFilePath, CancellationToken ct)
        {
            var batch = new List<TelemetryRecord>();

            while (!queue.IsCompleted)
            {
                batch.Clear();

                // Block up to 10 seconds for the first item
                try
                {
                    if (queue.TryTake(out var first, TimeSpan.FromSeconds(10)))
                        batch.Add(first);
                    else
                        continue;
                }
                catch (InvalidOperationException) { break; } // CompleteAdding was called

                // Drain any remaining items without blocking
                while (queue.TryTake(out var item))
                    batch.Add(item);

                if (batch.Count == 0) continue;

                // Open, append, close — blobfuse doesn't support file sharing
                var sb = new StringBuilder();
                foreach (var r in batch)
                {
                    var msg = SanitizeCsvField(r.MessageText);
                    sb.AppendLine(
                        $"{r.QueryNumber},{r.UserEmail},{r.Timestamp:yyyy-MM-dd HH:mm:ss.fff},{r.StartTimeMs:F0},{r.DurationMs:F1},{r.Outcome},{msg},{r.ActiveUsers}");
                }

                try
                {
                    File.AppendAllText(logFilePath, sb.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[LogWriter] Error writing: {ex.Message}");
                }
            }
        }

        private static string SanitizeCsvField(string value)
        {
            if (string.IsNullOrEmpty(value)) return "";
            // Truncate long messages
            if (value.Length > 500) value = value[..500];
            // Replace problematic characters
            value = value.Replace("\r", " ").Replace("\n", " ");
            // Quote if contains comma, quote, or whitespace
            if (value.Contains(',') || value.Contains('"') || value.Contains(' '))
            {
                value = "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            return value;
        }

        private static void SimulateUserWithConnections(
            int userIndex, string[] queries, string email,
            int queriesPerBatch, int pauseMs, int pauseBetweenQueriesMs,
            AdomdConnection[] connections, string connStr,
            ConcurrentBag<QueryResult> results, Stopwatch testStart,
            BlockingCollection<TelemetryRecord>? telemetryQueue,
            int[] activeUsers, CancellationToken ct)
        {
            try
            {
                int iteration = 0;
                while (!ct.IsCancellationRequested)
                {
                    iteration++;
                    RunIteration(userIndex, email, iteration, queries, connections, connStr,
                        pauseBetweenQueriesMs, results, testStart, telemetryQueue, activeUsers, ct);
                    if (ct.IsCancellationRequested) break;
                    try { Task.Delay(pauseMs, ct).Wait(ct); }
                    catch (OperationCanceledException) { break; }
                }
            }
            finally
            {
                for (int c = 0; c < connections.Length; c++)
                    if (connections[c] != null) { connections[c].Close(); connections[c].Dispose(); }
            }
        }

        private static void RunIteration(
            int userIndex, string email, int iteration, string[] queries,
            AdomdConnection[] connections, string connStr, int pauseBetweenQueriesMs,
            ConcurrentBag<QueryResult> results, Stopwatch testStart,
            BlockingCollection<TelemetryRecord>? telemetryQueue,
            int[] activeUsers,
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

                        // On connection error, log failure, reconnect, and retry once
                        if (r.Error != null && r.Error.Contains("timed out or was lost"))
                        {
                            results.Add(r);
                            SubmitTelemetry(telemetryQueue, qi, email, r, activeUsers);

                            Console.WriteLine($"[User {userIndex}] Q{qi} iter {iter} connection lost, reconnecting...");
                            try
                            {
                                conn.Close();
                                conn.Dispose();
                                conn = new AdomdConnection(connStr);
                                conn.Open();
                            }
                            catch (Exception reconEx)
                            {
                                Console.WriteLine($"[User {userIndex}] Reconnect failed: {reconEx.Message}");
                                throw;
                            }

                            // Retry the query once on the new connection
                            r = ExecuteQuery(userIndex, qi, iter, queries[qi], conn, testStart);
                        }

                        results.Add(r);
                        SubmitTelemetry(telemetryQueue, qi, email, r, activeUsers);

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

        private static void SubmitTelemetry(BlockingCollection<TelemetryRecord>? telemetryQueue,
            int qi, string email, QueryResult r, int[] activeUsers)
        {
            if (telemetryQueue != null && !telemetryQueue.IsAddingCompleted)
            {
                var record = new TelemetryRecord
                {
                    QueryNumber = qi,
                    UserEmail = email,
                    Timestamp = DateTime.UtcNow,
                    StartTimeMs = r.StartTimeMs,
                    DurationMs = r.DurationMs,
                    Outcome = r.Error == null ? "Success" : "Error",
                    MessageText = r.Error ?? $"{r.RowCount} rows",
                    ActiveUsers = Volatile.Read(ref activeUsers[0]),
                };
                telemetryQueue.TryAdd(record);
            }
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
            int nUsers, int nQueries, string? logFilePath = null)
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

            if (logFilePath != null)
                stats["logFile"] = logFilePath;

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
