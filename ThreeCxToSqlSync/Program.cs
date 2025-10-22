using log4net;
using log4net.Config;
using Microsoft.Extensions.Configuration;
using System.Reflection;

namespace ThreeCxToSqlSync
{
    class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Program));

        private static string _postgreConnectionString = string.Empty;
        private static string _msSqlConnectionString = string.Empty;
        private static IEnumerable<IConfigurationSection>? _syncTables;

        static void Main(string[] args)
        {
            InitializeLogging();

            bool configLoaded = LoadConfiguration();
            if (!configLoaded)
            {
                Log.Error("Error loading configuration file.");
                return;
            }

            if (_syncTables == null)
            {
                Log.Error("No sync tables were loaded. Aborting.");
                return;
            }

            foreach (var table in _syncTables)
            {
                var tableName = table.Key;
                var syncType = table["SyncType"];

                if (string.IsNullOrEmpty(syncType))
                {
                    Log.Error($"SyncType for table '{tableName}' is not configured.");
                    continue;
                }

                Log.Info($"##### Syncing table '{tableName}' with SyncType '{syncType}' #####");

                try
                {
                    SyncTable(tableName, table);
                }
                catch (Exception ex)
                {
                    Log.Error($"An error occurred while syncing table '{tableName}': {ex.Message}", ex);
                }
            }

            Log.Info("##### Synchronization completed #####");
        }

        private static void InitializeLogging()
        {
            var assembly = Assembly.GetEntryAssembly();
            if (assembly == null)
            {
                Console.WriteLine("Could not get entry assembly for logging initialization.");
                return;
            }

            var logRepository = LogManager.GetRepository(assembly);
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
            Log.Info("Logging initialized successfully.");
        }

        private static bool LoadConfiguration()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            _postgreConnectionString = configuration.GetConnectionString("PostgreSqlConnection") ?? string.Empty;
            _msSqlConnectionString = configuration.GetConnectionString("MsSqlConnection") ?? string.Empty;
            _syncTables = configuration.GetSection("SyncTablesAndQueries").GetChildren();

            if (string.IsNullOrEmpty(_postgreConnectionString))
            {
                Log.Error("PostgreSQL connection string is missing.");
                return false;
            }

            if (string.IsNullOrEmpty(_msSqlConnectionString))
            {
                Log.Error("MS SQL connection string is missing.");
                return false;
            }

            if (!_syncTables.Any())
            {
                Log.Warn("No SyncTablesAndQueries found in configuration.");
                return false;
            }

            return true;
        }

        private static void SyncTable(string tableName, IConfigurationSection table)
        {
            var syncType = table["SyncType"];
            var selectQuery = table["SelectQuery"];
            var insertQuery = table["InsertQuery"];
            var deleteQuery = table["DeleteQuery"];

            if (string.IsNullOrEmpty(selectQuery) || string.IsNullOrEmpty(insertQuery))
            {
                Log.Error($"Missing queries for table '{tableName}'.");
                return;
            }

            var msSqlDb = new MsSqlDatabase(_msSqlConnectionString);
            var postgreDb = new PostgreSqlDatabase(_postgreConnectionString);

            switch (syncType)
            {
                case "Delete_and_Insert":
                    ExecuteDeleteAndInsert(msSqlDb, postgreDb, tableName, selectQuery, insertQuery, deleteQuery ?? string.Empty);
                    break;

                case "Insert_latest":
                    ExecuteInsertLatest(msSqlDb, postgreDb, tableName, selectQuery, insertQuery);
                    break;

                default:
                    ExecuteFullSync(msSqlDb, postgreDb, tableName, selectQuery, insertQuery, deleteQuery ?? string.Empty);
                    break;
            }
        }

        private static void ExecuteDeleteAndInsert(MsSqlDatabase msSqlDb, PostgreSqlDatabase postgreDb,
            string tableName, string selectQuery, string insertQuery, string deleteQuery)
        {
            if (string.IsNullOrEmpty(deleteQuery))
            {
                Log.Error($"Delete query for '{tableName}' is not configured.");
                return;
            }

            int deleted = msSqlDb.ExecuteDeleteQuery(deleteQuery);
            Log.Info($"{deleted} rows deleted from '{tableName}'.");

            var dataTable = postgreDb.ExecuteSelectQuery(selectQuery, null);
            if (dataTable.Rows.Count > 0)
            {
                msSqlDb.ExecuteInsertQuery(insertQuery, dataTable);
                Log.Info($"{dataTable.Rows.Count} rows inserted into '{tableName}'.");
            }
            else
            {
                Log.Info($"No rows found to insert for '{tableName}'.");
            }
        }

        private static void ExecuteInsertLatest(MsSqlDatabase msSqlDb, PostgreSqlDatabase postgreDb,
            string tableName, string selectQuery, string insertQuery)
        {
            var latestData = msSqlDb.GetLatestEndTime(tableName);
            var latestEndTime = latestData.Item1;
            var dataTable = postgreDb.ExecuteSelectQuery(selectQuery, latestEndTime);

            if (dataTable.Rows.Count > 0)
            {
                msSqlDb.ExecuteInsertQuery(insertQuery, dataTable);
                Log.Info($"{dataTable.Rows.Count} new rows transferred for '{tableName}'.");
            }
            else
            {
                Log.Info($"No new rows to transfer for '{tableName}'.");
            }
        }

        private static void ExecuteFullSync(MsSqlDatabase msSqlDb, PostgreSqlDatabase postgreDb,
            string tableName, string selectQuery, string insertQuery, string deleteQuery)
        {
            if (string.IsNullOrEmpty(deleteQuery))
            {
                Log.Error($"Delete query for '{tableName}' is not configured.");
                return;
            }

            int deleted = msSqlDb.ExecuteDeleteQuery(deleteQuery);
            Log.Info($"{deleted} rows deleted for full sync on '{tableName}'.");

            var dataTable = postgreDb.ExecuteSelectQuery(selectQuery, null);
            if (dataTable.Rows.Count > 0)
            {
                msSqlDb.ExecuteInsertQuery(insertQuery, dataTable);
                Log.Info($"{dataTable.Rows.Count} rows inserted into '{tableName}'.");
            }
            else
            {
                Log.Info($"No data available for '{tableName}'.");
            }
        }
    }
}
