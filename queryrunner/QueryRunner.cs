using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.AnalysisServices.AdomdClient;

namespace Datagen
{
    /// <summary>
    /// Result of a single query execution.
    /// </summary>
    public class QueryResult
    {
        public int Index { get; set; }
        public int UserIndex { get; set; }
        public int QueryIndex { get; set; }
        public int RowCount { get; set; }
        public double DurationMs { get; set; }
        public string? Error { get; set; }
    }

    /// <summary>
    /// Runs DAX queries in parallel against an XMLA endpoint using ADOMD.NET.
    /// Each virtual user gets its own connection.
    /// </summary>
    public static class QueryRunner
    {
        /// <summary>
        /// Run a batch of queries in parallel.
        /// Each query gets its own connection (built from connectionStrings[i]).
        /// </summary>
        /// <param name="queries">DAX query texts</param>
        /// <param name="connectionStrings">Connection string per query (may repeat for same user)</param>
        /// <param name="maxParallelism">Max concurrent queries</param>
        /// <param name="useXmlReader">If true, use ExecuteXmlReader (faster, count rows only)</param>
        /// <returns>Array of QueryResult</returns>
        public static QueryResult[] RunBatch(
            string[] queries,
            string[] connectionStrings,
            int maxParallelism = 8,
            bool useXmlReader = false)
        {
            var results = new QueryResult[queries.Length];
            var options = new ParallelOptions { MaxDegreeOfParallelism = maxParallelism };

            Parallel.For(0, queries.Length, options, i =>
            {
                results[i] = RunSingle(i, queries[i], connectionStrings[i], useXmlReader);
            });

            return results;
        }

        /// <summary>
        /// Run a single query with its own connection, measuring duration and row count.
        /// </summary>
        private static QueryResult RunSingle(int index, string query, string connStr, bool useXmlReader)
        {
            var result = new QueryResult { Index = index };
            try
            {
                using var conn = new AdomdConnection(connStr);
                conn.Open();

                var cmd = new AdomdCommand(query, conn);
                var sw = Stopwatch.StartNew();

                if (useXmlReader)
                {
                    result.RowCount = ExecuteWithXmlReader(cmd);
                }
                else
                {
                    result.RowCount = ExecuteWithDataReader(cmd);
                }

                sw.Stop();
                result.DurationMs = sw.Elapsed.TotalMilliseconds;
            }
            catch (Exception ex)
            {
                result.Error = ex.Message;
                if (result.Error.Length > 200)
                    result.Error = result.Error.Substring(0, 200);
            }
            return result;
        }

        /// <summary>
        /// Standard ExecuteReader — reads all rows, counts them.
        /// </summary>
        private static int ExecuteWithDataReader(AdomdCommand cmd)
        {
            using var reader = cmd.ExecuteReader();
            int count = 0;
            while (reader.Read())
                count++;
            return count;
        }

        /// <summary>
        /// ExecuteXmlReader — parses the XMLA response as XML,
        /// counts row elements without converting column values.
        /// </summary>
        private static int ExecuteWithXmlReader(AdomdCommand cmd)
        {
            using var xmlReader = cmd.ExecuteXmlReader();
            int count = 0;

            // XMLA row response: <root><row>...</row><row>...</row></root>
            // The row element is in the urn:schemas-microsoft-com:xml-analysis:rowset namespace
            while (xmlReader.Read())
            {
                if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.LocalName == "row")
                {
                    count++;
                    // Skip the content of this row element entirely
                    xmlReader.Skip();
                }
            }

            return count;
        }

        /// <summary>
        /// Run a load test: each user runs all queries.
        /// Total executions = users.Length × queries.Length.
        /// </summary>
        /// <param name="queries">DAX query texts</param>
        /// <param name="xmlaEndpoint">XMLA endpoint URL</param>
        /// <param name="dataset">Semantic model name</param>
        /// <param name="token">Access token</param>
        /// <param name="userEmails">User emails for CustomData</param>
        /// <param name="userRoles">Role names per user</param>
        /// <param name="maxParallelism">Max concurrent queries across all users</param>
        /// <returns>Array of QueryResult (length = users × queries)</returns>
        public static QueryResult[] RunLoadTest(
            string[] queries,
            string xmlaEndpoint,
            string dataset,
            string token,
            string[] userEmails,
            string[] userRoles,
            int maxParallelism = 8)
        {
            int totalTasks = userEmails.Length * queries.Length;
            var results = new QueryResult[totalTasks];
            var options = new ParallelOptions { MaxDegreeOfParallelism = maxParallelism };

            // Build tasks: (taskIndex, userIndex, queryIndex)
            var tasks = new (int taskIdx, int userIdx, int queryIdx)[totalTasks];
            int t = 0;
            for (int u = 0; u < userEmails.Length; u++)
                for (int q = 0; q < queries.Length; q++)
                    tasks[t++] = (t - 1, u, q);

            // Shuffle to interleave users (avoid all queries for one user running together)
            var rng = new Random(42);
            for (int i = tasks.Length - 1; i > 0; i--)
            {
                int j = rng.Next(i + 1);
                (tasks[i], tasks[j]) = (tasks[j], tasks[i]);
            }

            Parallel.ForEach(tasks, options, task =>
            {
                string connStr =
                    $"Data Source={xmlaEndpoint};Initial Catalog={dataset};" +
                    $"password={token};Timeout=7200;" +
                    $"CustomData={userEmails[task.userIdx]};Roles={userRoles[task.userIdx]};";

                var r = RunSingle(task.queryIdx, queries[task.queryIdx], connStr, false);
                r.Index = task.taskIdx;
                r.UserIndex = task.userIdx;
                r.QueryIndex = task.queryIdx;
                results[task.taskIdx] = r;
            });

            return results;
        }

        /// <summary>
        /// Benchmark: run the same query N times sequentially, then in parallel,
        /// comparing ExecuteReader vs ExecuteXmlReader.
        /// Returns a summary string.
        /// </summary>
        public static string Benchmark(string query, string connStr, int nQueries = 20, int threads = 4)
        {
            var lines = new List<string>();

            // Warm up
            RunSingle(0, query, connStr, false);

            // Sequential - DataReader
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < nQueries; i++)
                RunSingle(i, query, connStr, false);
            sw.Stop();
            double seqReaderMs = sw.Elapsed.TotalMilliseconds;
            double seqReaderQps = nQueries / (seqReaderMs / 1000);
            lines.Add($"Sequential DataReader: {seqReaderMs:F0}ms, {seqReaderQps:F1} QPS");

            // Sequential - XmlReader
            sw.Restart();
            for (int i = 0; i < nQueries; i++)
                RunSingle(i, query, connStr, true);
            sw.Stop();
            double seqXmlMs = sw.Elapsed.TotalMilliseconds;
            double seqXmlQps = nQueries / (seqXmlMs / 1000);
            lines.Add($"Sequential XmlReader:  {seqXmlMs:F0}ms, {seqXmlQps:F1} QPS");

            // Parallel - DataReader
            var queries = Enumerable.Repeat(query, nQueries).ToArray();
            var connStrs = Enumerable.Repeat(connStr, nQueries).ToArray();

            sw.Restart();
            RunBatch(queries, connStrs, threads, false);
            sw.Stop();
            double parReaderMs = sw.Elapsed.TotalMilliseconds;
            double parReaderQps = nQueries / (parReaderMs / 1000);
            lines.Add($"Parallel({threads}) DataReader: {parReaderMs:F0}ms, {parReaderQps:F1} QPS, speedup={seqReaderMs/parReaderMs:F2}x");

            // Parallel - XmlReader
            sw.Restart();
            RunBatch(queries, connStrs, threads, true);
            sw.Stop();
            double parXmlMs = sw.Elapsed.TotalMilliseconds;
            double parXmlQps = nQueries / (parXmlMs / 1000);
            lines.Add($"Parallel({threads}) XmlReader:  {parXmlMs:F0}ms, {parXmlQps:F1} QPS, speedup={seqXmlMs/parXmlMs:F2}x");

            return string.Join("\n", lines);
        }
    }
}
