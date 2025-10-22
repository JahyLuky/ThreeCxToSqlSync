# ThreeCxToSqlSync

This .NET console application synchronizes data from a PostgreSQL database, specifically from a 3CX phone system, to a Microsoft SQL Server database. It's designed to be flexible and configurable, allowing you to synchronize multiple tables using different strategies.

## How It Works

The application reads its configuration from the `appsettings.json` file. This file defines the connection strings for the source (PostgreSQL) and destination (MS SQL Server) databases, as well as the tables to be synchronized.

For each table, you can specify a `SyncType` to control the synchronization behavior:

*   **`Insert_latest`**: This strategy only adds new rows to the destination table. It determines the latest record in the destination table by looking at the `cdr_ended_at` timestamp and fetches only newer records from the source. This is ideal for synchronizing call detail records (CDRs) without re-processing old data.
*   **`Delete_and_Insert`**: This strategy first executes a custom `DELETE` query on the destination table and then inserts all data from the source table. This is useful for tables that need to be completely refreshed but where a simple `TRUNCATE` is not sufficient.
*   **Default (Full Sync)**: If the `SyncType` is anything other than the two above, the application performs a full synchronization. It first deletes all existing data from the destination table and then inserts all data from the source table.

## Prerequisites

*   .NET 9.0 SDK (or a compatible runtime)
*   Access to a PostgreSQL database (source)
*   Access to a Microsoft SQL Server database (destination)

## Configuration

The `appsettings.json` file has the following structure:

```json
{
  "ConnectionStrings": {
    "PostgreSqlConnection": "Server=your_postgres_server;Port=port;Database=your_database;User Id=your_user;Password=your_password;",
    "MsSqlConnection": "Server=your_sql_server;Database=your_database;User Id=your_user;Password=your_password;Trusted_Connection=False;"
  },
  "SyncTablesAndQueries": {
    "cdroutput": {
      "SyncType": "Insert_latest",
      "SelectQuery": "SELECT call_id, caller_id, destination, call_duration, talking_duration, start_time, end_time, call_type, status FROM public.call_history",
      "InsertQuery": "INSERT INTO cdroutput (call_id, caller_id, destination, call_duration, talking_duration, start_time, end_time, call_type, status) VALUES (@value1, @value2, @value3, @value4, @value5, @value6, @value7, @value8, @value9)"
    },
    "users": {
      "SyncType": "Delete_and_Insert",
      "SelectQuery": "SELECT id, first_name, last_name, email, extension FROM public.users",
      "InsertQuery": "INSERT INTO users (id, first_name, last_name, email, extension) VALUES (@value1, @value2, @value3, @value4, @value5)",
      "DeleteQuery": "DELETE FROM users"
    }
  }
}
```

### `ConnectionStrings`

*   **`PostgreSqlConnection`**: The connection string for the source PostgreSQL database.
*   **`MsSqlConnection`**: The connection string for the destination Microsoft SQL Server database.

### `SyncTablesAndQueries`

This section contains a list of tables to be synchronized. Each table is an object identified by its name (e.g., `cdroutput`, `users`).

For each table, you must specify the following properties:

*   **`SyncType`**: The synchronization strategy. Can be `Insert_latest`, `Delete_and_Insert`, or any other value for a full sync.
*   **`SelectQuery`**: The `SELECT` query to be executed on the source PostgreSQL database.
*   **`InsertQuery`**: The `INSERT` query to be executed on the destination Microsoft SQL Server database. The values must be represented as `@value1`, `@value2`, etc., corresponding to the order of columns in the `SelectQuery`.
*   **`DeleteQuery`**: The `DELETE` query to be executed on the destination Microsoft SQL Server database. This property is required for `Delete_and_Insert` and full sync operations.

## How to Run the Application

1.  **Configure `appsettings.json`**: Update the connection strings and synchronization settings.
2.  **Restore Dependencies**: Open a terminal in the project root and run:
    ```bash
    dotnet restore
    ```
3.  **Build the Project**:
    ```bash
    dotnet build
    ```
4.  **Run the Application**:
    ```bash
    dotnet run
    ```

The application can also be published as a single-file executable by running `dotnet publish --configuration Release`. This will create a self-contained executable in the `bin/Release/net9.0/win-x64/publish` directory.