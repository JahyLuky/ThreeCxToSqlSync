using log4net;
using Microsoft.Data.SqlClient;
using System.Data;

namespace ThreeCxToSqlSync
{
    public class MsSqlDatabase
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(MsSqlDatabase));
        private readonly string _connectionString;

        public MsSqlDatabase(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void ExecuteInsertQuery(string insertQuery, DataTable dataTable)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                Log.Info("Connected to MS SQL Server.");
                Log.Info($"MSSQL query: {insertQuery}");
                foreach (DataRow row in dataTable.Rows)
                {
                    using (var command = new SqlCommand(insertQuery, connection))
                    {
                        for (int i = 0; i < dataTable.Columns.Count; i++)
                        {
                            command.Parameters.AddWithValue($"@value{i + 1}", row[i]);
                        }

                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        public Tuple<DateTimeOffset?, Guid?> GetLatestEndTime(string tableName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                Log.Info($"Connected to MS SQL Server to get the latest end time for table '{tableName}'.");

                using (var command = new SqlCommand($"SELECT TOP 1 cdr_ended_at, cdr_id FROM {tableName} ORDER BY cdr_ended_at DESC", connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var latestEndTime = reader.GetDateTimeOffset(0);
                            var latestId = reader.GetGuid(1);
                            Log.Info($"Latest end time from '{tableName}' is {latestEndTime} with id {latestId}");
                            return new Tuple<DateTimeOffset?, Guid?>(latestEndTime, latestId);
                        }
                    }
                }
            }
            Log.Info($"No end time found in '{tableName}', returning nulls");
            return new Tuple<DateTimeOffset?, Guid?>(null, null);
        }

        public int ExecuteDeleteQuery(string deleteQuery)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                Log.Info($"Executing query: {deleteQuery}");

                using (var command = new SqlCommand(deleteQuery, connection))
                {
                    int affectedRows = command.ExecuteNonQuery();
                    Log.Info($"Delete query affected {affectedRows} rows.");
                    return affectedRows;
                }
            }
        }
    }
}
