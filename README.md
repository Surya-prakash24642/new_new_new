# SQL-to-Lakehouse Data Ingestion Framework

A robust, scalable framework for ingesting data from Azure SQL databases into a Delta Lake-powered data lakehouse with support for full, incremental, and hash-based loading strategies.

## Features

- **Multiple Loading Strategies**: 
  - Full Load (FL): Complete table refresh
  - Incremental Load (IL): Based on timestamp/date field
  - Hash-based Load: Row-level change detection
  
- **Hard Delete Detection**: Automatically identifies and marks records deleted from the source
- **Metadata Tracking**: Each record includes loading timestamp and deletion status
- **Binary Data Handling**: Automatic Base64 encoding for VARBINARY columns
- **Parallel Processing**: Multi-threaded execution for faster data ingestion
- **Comprehensive Logging**: Detailed logging of all operations

## Prerequisites

- Apache Spark environment (Databricks, Synapse Analytics, or standalone Spark cluster)
- PySpark 3.0 or higher
- Access credentials for Azure SQL Database
- Delta Lake installed in your Spark environment
- Python libraries: concurrent.futures, datetime, base64

## Setup Instructions

### 1. Configure Connection Parameters

Create environment variables or securely store the following:

```python
jdbc_hostname = "your-azure-sql-server.database.windows.net"
jdbc_port = "1433"
jdbc_database = "your_database_name"
jdbc_username = "your_username"
jdbc_password = "your_password"
```

### 2. Prepare Configuration File

Create a CSV file named `connector_sql.csv` with the following columns:

| Column | Description |
|--------|-------------|
| source_name | Friendly name for the data source |
| load_type | Loading strategy: FL (Full Load), IL (Incremental Load), or HASH |
| source_schema | Schema name in Azure SQL Server |
| object_name | Table name in Azure SQL Server |
| incremental_field | Field used for incremental loads (timestamp/date column) |
| primary_key | Primary key field(s), pipe-delimited for composite keys |
| sink_schema | Target schema in the data lakehouse |

Example row:
```
"Sales Database","IL","dbo","Customers","LastModifiedDate","CustomerId","raw.connector_sql"
```

### 3. Create Lakehouse Tables

Ensure your Delta Lake has the following tables:
- Tables for your ingested data (will be created automatically)
- Log table: `raw.connector_sql` (for ingestion logs)
- Hard delete log table: `catalog.raw.connector_sql.vp_monitor_hard_delete_log`

## Usage Guide

### Basic Usage

1. Place the script and configuration file in your Spark environment
2. Set up connection parameters
3. Run the script:

```python
spark-submit sql_lakehouse_ingestion.py
```

### Loading Strategies

#### Full Load (FL)
- Overwrites the entire target table
- Use for small tables or initial loads
- Configuration: set `load_type` to `FL`

#### Incremental Load (IL)
- Appends only new records based on a timestamp field
- Requires `incremental_field` to be set
- Configuration: set `load_type` to `IL`

#### Hash-based Load (HASH)
- Compares record checksums to identify changes
- Most efficient for large tables with infrequent changes
- Configuration: set `load_type` to `HASH`

### Hard Delete Detection

The framework automatically detects records deleted from the source by:
1. Comparing primary keys between source and target
2. Marking deleted records with `__is_deleted = True`
3. Setting `__deleted_at` timestamp

This preserves historical data while flagging records no longer in the source.

## Understanding Output Tables

Each ingested table includes metadata columns:

| Column | Description |
|--------|-------------|
| __is_deleted | Boolean indicating if record has been deleted from source |
| __deleted_at | Timestamp when record was detected as deleted |
| __data_loaded_at | Timestamp when record was loaded |

Target tables are named using the pattern: `{source_schema}__{source_table}`

## Monitoring and Logging

- Ingestion logs are written to `raw.connector_sql`
- Hard delete detection logs are written to `catalog.raw.connector_sql.vp_monitor_hard_delete_log`

## Troubleshooting

### Common Issues

1. **Connection Failures**:
   - Verify JDBC connection parameters
   - Check firewall rules for your Azure SQL Database

2. **Schema Mismatch Errors**:
   - For the first run, use FL load type to establish schema
   - For HASH loading, the framework handles schema evolution

3. **Parallel Processing Issues**:
   - Adjust `max_workers` in the ThreadPoolExecutor to balance performance and resource usage
