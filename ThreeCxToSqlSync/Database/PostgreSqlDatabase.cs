using log4net;
using Npgsql;
using System.Data;

namespace ThreeCxToSqlSync
{
    public class PostgreSqlDatabase
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(PostgreSqlDatabase));
        private readonly string _connectionString;

        public PostgreSqlDatabase(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DataTable ExecuteSelectQuery(string selectQuery, DateTimeOffset? latestEndTime)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();
                Log.Info("Connected to PostgreSQL.");

                var query = selectQuery;
                if (latestEndTime.HasValue)
                {
                    query += " WHERE date_trunc('second', cdr_ended_at) > @latestEndTime";
                    query += " ORDER BY cdr_ended_at";
                }

                Log.Info($"Executing query: {query}");

                using (var command = new NpgsqlCommand(query, connection))
                {
                    if (latestEndTime.HasValue)
                    {
                        var latestEndTimeValue = latestEndTime.Value;
                        var truncatedTime = new DateTimeOffset(latestEndTimeValue.Year, latestEndTimeValue.Month, latestEndTimeValue.Day, latestEndTimeValue.Hour, latestEndTimeValue.Minute, latestEndTimeValue.Second, latestEndTimeValue.Offset);
                        command.Parameters.AddWithValue("@latestEndTime", truncatedTime);
                    }

                    using (var reader = command.ExecuteReader())
                    {
                        var dataTable = new DataTable();
                        dataTable.Load(reader);
                        return dataTable;
                    }
                }
            }
        }
    }
}
