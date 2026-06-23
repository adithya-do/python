#Requires -Version 5.1
<#
.SYNOPSIS
    SQL Server Performance Analysis Script - Problem SQLs, Index Issues & Recommendations

.DESCRIPTION
    Connects to a SQL Server instance using Windows Authentication and analyzes:
      - Top CPU / IO / Duration offending queries from the Plan Cache & Query Store
      - Missing index recommendations (sys.dm_db_missing_index_*)
      - Unused indexes (wasting write overhead)
      - Duplicate / redundant indexes
      - Index fragmentation for indexes above a configurable size threshold
      - Wait statistics over the analysis window
    Outputs a timestamped HTML report to the current directory.

.PARAMETER ServerName
    SQL Server instance name or host\instance (e.g. "MYSRV" or "MYSRV\SQL2019").

.PARAMETER DatabaseName
    Target database name.

.PARAMETER StartTime
    Start of the analysis window (date + time). Accepts any format PowerShell can parse,
    e.g. "2025-06-20 08:00", "06/20/2025 08:00:00", "2025-06-20T08:00:00".
    Defaults to 24 hours before the current time if omitted.

.PARAMETER EndTime
    End of the analysis window (date + time). Defaults to the current time if omitted.
    Must be later than StartTime.

.PARAMETER TopN
    Number of top offending queries to surface per category (default: 20).

.PARAMETER FragmentationThresholdPct
    Minimum avg fragmentation % to flag an index (default: 10).

.PARAMETER MinIndexSizePages
    Minimum index size in pages before fragmentation is checked (default: 128 = 1 MB).

.PARAMETER OutputPath
    Directory for the HTML report (default: current directory).

.EXAMPLE
    # Explicit window
    .\Invoke-SQLPerfAnalysis.ps1 -ServerName "MYSRV" -DatabaseName "AdventureWorks" `
        -StartTime "2025-06-20 08:00" -EndTime "2025-06-20 18:00"

.EXAMPLE
    # Last 48 hours (omit both dates – defaults kick in)
    .\Invoke-SQLPerfAnalysis.ps1 -ServerName "MYSRV\SQL2019" -DatabaseName "Sales" `
        -StartTime (Get-Date).AddHours(-48) -TopN 30 -OutputPath "C:\Reports"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$ServerName,

    [Parameter(Mandatory)]
    [string]$DatabaseName,

    [Parameter()]
    [Nullable[datetime]]$StartTime = $null,   # defaults to (now – 24 h) if not supplied

    [Parameter()]
    [Nullable[datetime]]$EndTime   = $null,   # defaults to now if not supplied

    [Parameter()]
    [int]$TopN = 20,

    [Parameter()]
    [double]$FragmentationThresholdPct = 10.0,

    [Parameter()]
    [int]$MinIndexSizePages = 128,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ─────────────────────────────────────────────────────────────────────────────
# Resolve & validate the analysis window
# ─────────────────────────────────────────────────────────────────────────────
$Now = Get-Date

if ($null -eq $EndTime)   { $EndTime   = $Now }
if ($null -eq $StartTime) { $StartTime = $EndTime.AddHours(-24) }

if ($StartTime -ge $EndTime) {
    throw "StartTime ($StartTime) must be earlier than EndTime ($EndTime)."
}
if ($EndTime -gt $Now.AddMinutes(1)) {
    Write-Warning "EndTime ($EndTime) is in the future – clamping to now."
    $EndTime = $Now
}

# T-SQL-safe literal strings  (ODBC canonical format – works on every locale)
$sqlStart = $StartTime.ToString("yyyy-MM-dd HH:mm:ss")
$sqlEnd   = $EndTime.ToString("yyyy-MM-dd HH:mm:ss")

$windowDesc = "$($StartTime.ToString('yyyy-MM-dd HH:mm')) → $($EndTime.ToString('yyyy-MM-dd HH:mm'))"
$windowMins = [int]($EndTime - $StartTime).TotalMinutes

# ─────────────────────────────────────────────────────────────────────────────
# Helper – run a T-SQL query and return a DataTable
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-SqlQuery {
    param(
        [string]$ConnectionString,
        [string]$Query,
        [int]$TimeoutSeconds = 120
    )
    $conn = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
    try {
        $conn.Open()
        $cmd  = $conn.CreateCommand()
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $TimeoutSeconds
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
        $dt      = New-Object System.Data.DataTable
        [void]$adapter.Fill($dt)
        return $dt
    }
    finally {
        $conn.Close()
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Helper – convert a DataTable to an HTML <table>
# ─────────────────────────────────────────────────────────────────────────────
function ConvertTo-HtmlTable {
    param(
        [System.Data.DataTable]$DataTable,
        [string]$EmptyMessage = "No data returned."
    )
    if ($null -eq $DataTable -or $DataTable.Rows.Count -eq 0) {
        return "<p class='empty'>$EmptyMessage</p>"
    }
    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.Append("<table><thead><tr>")
    foreach ($col in $DataTable.Columns) {
        [void]$sb.Append("<th>$([System.Web.HttpUtility]::HtmlEncode($col.ColumnName))</th>")
    }
    [void]$sb.Append("</tr></thead><tbody>")
    foreach ($row in $DataTable.Rows) {
        [void]$sb.Append("<tr>")
        foreach ($col in $DataTable.Columns) {
            $val = $row[$col.ColumnName]
            if ($val -is [DBNull]) { $val = "" }
            [void]$sb.Append("<td>$([System.Web.HttpUtility]::HtmlEncode($val.ToString()))</td>")
        }
        [void]$sb.Append("</tr>")
    }
    [void]$sb.Append("</tbody></table>")
    return $sb.ToString()
}

# ─────────────────────────────────────────────────────────────────────────────
# Load System.Web for HtmlEncode
# ─────────────────────────────────────────────────────────────────────────────
Add-Type -AssemblyName System.Web

# ─────────────────────────────────────────────────────────────────────────────
# Connection strings
# ─────────────────────────────────────────────────────────────────────────────
$masterConnStr = "Server=$ServerName;Database=master;Integrated Security=True;Application Name=PerfAnalysis;"
$dbConnStr     = "Server=$ServerName;Database=$DatabaseName;Integrated Security=True;Application Name=PerfAnalysis;"

Write-Host "`n[$(Get-Date -f 'HH:mm:ss')] Connecting to $ServerName  |  DB: $DatabaseName" -ForegroundColor Cyan
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Analysis window : $windowDesc  ($windowMins minutes)" -ForegroundColor Cyan

# ─────────────────────────────────────────────────────────────────────────────
# Verify connectivity and capture server info
# ─────────────────────────────────────────────────────────────────────────────
$serverInfoSql = @"
SELECT
    SERVERPROPERTY('ServerName')       AS [ServerName],
    SERVERPROPERTY('ProductVersion')   AS [Version],
    SERVERPROPERTY('Edition')          AS [Edition],
    SERVERPROPERTY('ProductLevel')     AS [ServicePack],
    CAST(DATABASEPROPERTYEX('$DatabaseName','Collation') AS NVARCHAR(128)) AS [DBCollation],
    (SELECT compatibility_level FROM sys.databases WHERE name = '$DatabaseName') AS [CompatLevel]
"@
$serverInfo = Invoke-SqlQuery -ConnectionString $masterConnStr -Query $serverInfoSql

Write-Host "[$(Get-Date -f 'HH:mm:ss')] Server: $($serverInfo.Rows[0]['ServerName'])  Version: $($serverInfo.Rows[0]['Version'])" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────────────────────
# Detect whether Query Store is enabled for this DB
# ─────────────────────────────────────────────────────────────────────────────
$qsCheckSql = "SELECT actual_state FROM sys.database_query_store_options WHERE actual_state IN (1,2)"
$qsCheck    = Invoke-SqlQuery -ConnectionString $dbConnStr -Query $qsCheckSql
$hasQS      = ($qsCheck.Rows.Count -gt 0)
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Query Store enabled: $hasQS" -ForegroundColor $(if ($hasQS) {"Green"} else {"Yellow"})

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 – Top queries from Plan Cache (works on all versions)
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Collecting plan cache data..." -ForegroundColor Cyan

$planCacheSql = @"
;WITH CacheData AS (
    SELECT TOP ($TopN)
         qs.total_worker_time        / 1000000.0   AS [Total_CPU_sec],
         qs.total_worker_time        / NULLIF(qs.execution_count,0) / 1000.0 AS [Avg_CPU_ms],
         qs.total_elapsed_time       / 1000000.0   AS [Total_Duration_sec],
         qs.total_elapsed_time       / NULLIF(qs.execution_count,0) / 1000.0 AS [Avg_Duration_ms],
         qs.total_logical_reads                     AS [Total_LogicalReads],
         qs.total_logical_reads      / NULLIF(qs.execution_count,0) AS [Avg_LogicalReads],
         qs.total_logical_writes                    AS [Total_LogicalWrites],
         qs.execution_count                         AS [Executions],
         qs.creation_time                           AS [Plan_Compiled_At],
         SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
             ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
               ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1
         )                                          AS [Statement_Text],
         DB_NAME(qp.dbid)                           AS [Database],
         OBJECT_NAME(qp.objectid, qp.dbid)          AS [Object_Name]
    FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle)  AS st
    CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
    WHERE qs.creation_time BETWEEN '$sqlStart' AND '$sqlEnd'
      AND (DB_NAME(qp.dbid) = '$DatabaseName' OR st.text LIKE '%$DatabaseName%')
)
SELECT * FROM CacheData
ORDER BY Total_CPU_sec DESC
"@

$planCacheData = Invoke-SqlQuery -ConnectionString $masterConnStr -Query $planCacheSql

# Top by Logical Reads (I/O pressure)
$planCacheIOSql = @"
SELECT TOP ($TopN)
     qs.total_logical_reads / NULLIF(qs.execution_count,0) AS [Avg_LogicalReads],
     qs.total_logical_reads                                  AS [Total_LogicalReads],
     qs.execution_count                                      AS [Executions],
     qs.total_worker_time / NULLIF(qs.execution_count,0) / 1000.0 AS [Avg_CPU_ms],
     qs.total_elapsed_time/ NULLIF(qs.execution_count,0) / 1000.0 AS [Avg_Duration_ms],
     qs.creation_time                                        AS [Plan_Compiled_At],
     SUBSTRING(st.text,(qs.statement_start_offset/2)+1,
         ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
           ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1) AS [Statement_Text],
     DB_NAME(qp.dbid)                                       AS [Database],
     OBJECT_NAME(qp.objectid, qp.dbid)                      AS [Object_Name]
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle)  AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qs.creation_time BETWEEN '$sqlStart' AND '$sqlEnd'
  AND (DB_NAME(qp.dbid) = '$DatabaseName' OR st.text LIKE '%$DatabaseName%')
ORDER BY Avg_LogicalReads DESC
"@
$planCacheIOData = Invoke-SqlQuery -ConnectionString $masterConnStr -Query $planCacheIOSql

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 – Query Store top queries (SQL Server 2016+ / compat 130+)
# ─────────────────────────────────────────────────────────────────────────────
$qsTopCpuData  = $null
$qsTopIOData   = $null
if ($hasQS) {
    Write-Host "[$(Get-Date -f 'HH:mm:ss')] Collecting Query Store data..." -ForegroundColor Cyan

    $qsTopCpuSql = @"
SELECT TOP ($TopN)
    qsq.query_id                                                   AS [Query_ID],
    ROUND(SUM(qsrs.avg_cpu_time)     / 1000.0, 2)                AS [Avg_CPU_ms],
    ROUND(SUM(qsrs.avg_duration)     / 1000.0, 2)                AS [Avg_Duration_ms],
    ROUND(SUM(qsrs.avg_logical_io_reads), 0)                      AS [Avg_LogicalReads],
    SUM(qsrs.count_executions)                                     AS [Total_Executions],
    MAX(qsq.last_execution_time)                                   AS [Last_Execution],
    LEFT(qsqt.query_sql_text, 2000)                               AS [Query_Text],
    OBJECT_NAME(qsq.object_id)                                     AS [Object_Name]
FROM sys.query_store_query            AS qsq
JOIN sys.query_store_query_text       AS qsqt ON qsqt.query_text_id = qsq.query_text_id
JOIN sys.query_store_plan             AS qsp  ON qsp.query_id        = qsq.query_id
JOIN sys.query_store_runtime_stats    AS qsrs ON qsrs.plan_id        = qsp.plan_id
JOIN sys.query_store_runtime_stats_interval AS qsi
     ON qsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
WHERE qsi.start_time BETWEEN '$sqlStart' AND '$sqlEnd'
GROUP BY qsq.query_id, qsqt.query_sql_text, qsq.object_id
ORDER BY Avg_CPU_ms DESC
"@
    $qsTopCpuData = Invoke-SqlQuery -ConnectionString $dbConnStr -Query $qsTopCpuSql

    $qsTopIOSql = $qsTopCpuSql -replace "ORDER BY Avg_CPU_ms DESC","ORDER BY Avg_LogicalReads DESC"
    $qsTopIOData = Invoke-SqlQuery -ConnectionString $dbConnStr -Query $qsTopIOSql

    # Queries with high plan count (plan instability / parameter sniffing)
    $qsPlanInstabilitySql = @"
SELECT TOP 20
    qsq.query_id,
    COUNT(DISTINCT qsp.plan_id)     AS [Plan_Count],
    LEFT(qsqt.query_sql_text, 1000) AS [Query_Text],
    OBJECT_NAME(qsq.object_id)      AS [Object_Name]
FROM sys.query_store_query      AS qsq
JOIN sys.query_store_query_text AS qsqt ON qsqt.query_text_id = qsq.query_text_id
JOIN sys.query_store_plan       AS qsp  ON qsp.query_id        = qsq.query_id
GROUP BY qsq.query_id, qsqt.query_sql_text, qsq.object_id
HAVING COUNT(DISTINCT qsp.plan_id) > 3
ORDER BY Plan_Count DESC
"@
    $qsPlanInstabilityData = Invoke-SqlQuery -ConnectionString $dbConnStr -Query $qsPlanInstabilitySql
}

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 – Missing Index Recommendations
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Collecting missing index recommendations..." -ForegroundColor Cyan

$missingIndexSql = @"
SELECT TOP 50
    ROUND(migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans), 2)
                                                           AS [Improvement_Score],
    migs.user_seeks                                        AS [User_Seeks],
    migs.user_scans                                        AS [User_Scans],
    ROUND(migs.avg_user_impact, 2)                        AS [Avg_Impact_Pct],
    mig.equality_columns                                   AS [Equality_Columns],
    mig.inequality_columns                                 AS [Inequality_Columns],
    mig.included_columns                                   AS [Include_Columns],
    mid.statement                                          AS [Table],
    '-- Estimated Benefit: ' + CAST(ROUND(migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans),0) AS VARCHAR)
    + CHAR(10) + 'CREATE INDEX [IX_<name>] ON ' + mid.statement
    + ' (' + ISNULL(mig.equality_columns,'')
    + CASE WHEN mig.equality_columns IS NOT NULL AND mig.inequality_columns IS NOT NULL THEN ', ' ELSE '' END
    + ISNULL(mig.inequality_columns,'') + ')'
    + CASE WHEN mig.included_columns IS NOT NULL
           THEN ' INCLUDE (' + mig.included_columns + ')' ELSE '' END
    + ';'                                                  AS [Create_Index_DDL]
FROM sys.dm_db_missing_index_group_stats migs
JOIN sys.dm_db_missing_index_groups      mig  ON migs.group_handle = mig.index_group_handle
JOIN sys.dm_db_missing_index_details     mid  ON mig.index_handle  = mid.index_handle
WHERE mid.database_id = DB_ID('$DatabaseName')
ORDER BY Improvement_Score DESC
"@
$missingIndexData = Invoke-SqlQuery -ConnectionString $masterConnStr -Query $missingIndexSql

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 – Unused Indexes
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Collecting unused index data..." -ForegroundColor Cyan

$unusedIndexSql = @"
SELECT
    OBJECT_NAME(i.object_id)   AS [Table_Name],
    i.name                     AS [Index_Name],
    i.type_desc                AS [Index_Type],
    ius.user_seeks             AS [User_Seeks],
    ius.user_scans             AS [User_Scans],
    ius.user_lookups           AS [User_Lookups],
    ius.user_updates           AS [User_Updates],
    ius.last_user_seek         AS [Last_Seek],
    ius.last_user_scan         AS [Last_Scan],
    CAST(8.0 * SUM(a.used_pages) / 1024 AS DECIMAL(10,2)) AS [Size_MB],
    'DROP INDEX [' + i.name + '] ON [' + SCHEMA_NAME(t.schema_id) + '].[' + OBJECT_NAME(i.object_id) + '];'
                               AS [Drop_DDL]
FROM sys.indexes i
JOIN sys.objects t ON t.object_id = i.object_id
LEFT JOIN sys.dm_db_index_usage_stats ius
     ON ius.object_id = i.object_id AND ius.index_id = i.index_id AND ius.database_id = DB_ID()
LEFT JOIN sys.partitions p  ON p.object_id = i.object_id AND p.index_id = i.index_id
LEFT JOIN sys.allocation_units a ON a.container_id = p.partition_id
WHERE i.type_desc <> 'HEAP'
  AND i.is_primary_key   = 0
  AND i.is_unique        = 0
  AND i.is_unique_constraint = 0
  AND t.type = 'U'
  AND (ius.user_seeks IS NULL OR (ius.user_seeks = 0 AND ius.user_scans = 0 AND ius.user_lookups = 0))
  AND ius.user_updates > 0  -- being maintained but never read
GROUP BY i.object_id, i.name, i.type_desc, t.schema_id,
         ius.user_seeks, ius.user_scans, ius.user_lookups,
         ius.user_updates, ius.last_user_seek, ius.last_user_scan
ORDER BY ius.user_updates DESC
"@
$unusedIndexData = Invoke-SqlQuery -ConnectionString $dbConnStr -Query $unusedIndexSql

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 – Duplicate / Redundant Indexes
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Detecting duplicate indexes..." -ForegroundColor Cyan

$duplicateIndexSql = @"
;WITH IndexColumns AS (
    SELECT
        i.object_id,
        i.index_id,
        i.name       AS index_name,
        i.type_desc,
        STUFF((
            SELECT ', ' + COL_NAME(ic2.object_id, ic2.column_id)
            FROM sys.index_columns ic2
            WHERE ic2.object_id  = i.object_id
              AND ic2.index_id   = i.index_id
              AND ic2.is_included_column = 0
            ORDER BY ic2.key_ordinal
            FOR XML PATH('')
        ),1,2,'') AS key_cols,
        STUFF((
            SELECT ', ' + COL_NAME(ic2.object_id, ic2.column_id)
            FROM sys.index_columns ic2
            WHERE ic2.object_id  = i.object_id
              AND ic2.index_id   = i.index_id
              AND ic2.is_included_column = 1
            ORDER BY ic2.index_column_id
            FOR XML PATH('')
        ),1,2,'') AS inc_cols
    FROM sys.indexes i
    WHERE i.type_desc IN ('NONCLUSTERED','CLUSTERED')
      AND i.is_primary_key = 0
      AND i.is_unique_constraint = 0
)
SELECT
    OBJECT_NAME(a.object_id)    AS [Table_Name],
    a.index_name                AS [Index_A],
    b.index_name                AS [Index_B],
    a.type_desc                 AS [Type],
    a.key_cols                  AS [Key_Columns],
    a.inc_cols                  AS [Included_Columns_A],
    b.inc_cols                  AS [Included_Columns_B]
FROM IndexColumns a
JOIN IndexColumns b
     ON a.object_id  = b.object_id
    AND a.index_id  < b.index_id
    AND a.key_cols   = b.key_cols
ORDER BY [Table_Name], [Key_Columns]
"@
$duplicateIndexData = Invoke-SqlQuery -ConnectionString $dbConnStr -Query $duplicateIndexSql

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 – Index Fragmentation
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Checking index fragmentation (this may take a moment)..." -ForegroundColor Cyan

$fragmentationSql = @"
SELECT
    OBJECT_NAME(ips.object_id)          AS [Table_Name],
    i.name                              AS [Index_Name],
    i.type_desc                         AS [Index_Type],
    ips.partition_number                AS [Partition],
    ips.page_count                      AS [Page_Count],
    ROUND(ips.avg_fragmentation_in_percent, 2) AS [Avg_Fragmentation_Pct],
    ROUND(ips.avg_page_space_used_in_percent,2) AS [Avg_Page_Fill_Pct],
    CASE
        WHEN ips.avg_fragmentation_in_percent < 10  THEN 'OK – no action needed'
        WHEN ips.avg_fragmentation_in_percent < 30  THEN 'REORGANIZE recommended'
        ELSE                                              'REBUILD recommended'
    END                                 AS [Recommendation],
    CASE
        WHEN ips.avg_fragmentation_in_percent < 10  THEN ''
        WHEN ips.avg_fragmentation_in_percent < 30
        THEN 'ALTER INDEX [' + i.name + '] ON [' + SCHEMA_NAME(t.schema_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REORGANIZE;'
        ELSE 'ALTER INDEX [' + i.name + '] ON [' + SCHEMA_NAME(t.schema_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REBUILD WITH (ONLINE = ON, FILLFACTOR = 80);'
    END                                 AS [Maintenance_DDL]
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN sys.indexes i  ON i.object_id = ips.object_id AND i.index_id = ips.index_id
JOIN sys.objects t  ON t.object_id = ips.object_id
WHERE ips.avg_fragmentation_in_percent >= $FragmentationThresholdPct
  AND ips.page_count >= $MinIndexSizePages
  AND i.type > 0
  AND t.type = 'U'
ORDER BY ips.avg_fragmentation_in_percent DESC
"@
$fragmentationData = Invoke-SqlQuery -ConnectionString $dbConnStr -Query $fragmentationSql

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 – Wait Statistics
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Collecting wait statistics..." -ForegroundColor Cyan

$waitStatsSql = @"
SELECT TOP 25
    wait_type                                           AS [Wait_Type],
    waiting_tasks_count                                 AS [Wait_Count],
    ROUND(wait_time_ms / 1000.0, 2)                    AS [Total_Wait_sec],
    ROUND(wait_time_ms / NULLIF(waiting_tasks_count,0) / 1000.0, 4) AS [Avg_Wait_sec],
    ROUND(signal_wait_time_ms / 1000.0, 2)             AS [Signal_Wait_sec],
    ROUND(100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER(), 0), 2) AS [Pct_Total]
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'SLEEP_TASK','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_AUTO_EVENT',
    'DISPATCHER_QUEUE_SEMAPHORE','FT_IFTS_SCHEDULER_IDLE_WAIT',
    'HADR_FILESTREAM_IOMGR_IOCOMPLETION','HADR_WORK_QUEUE',
    'LAZYWRITER_SLEEP','LOGMGR_QUEUE','ONDEMAND_TASK_QUEUE',
    'REQUEST_FOR_DEADLOCK_SEARCH','RESOURCE_QUEUE','SERVER_IDLE_CHECK',
    'SLEEP_DBSTARTUP','SLEEP_DCOMSTARTUP','SLEEP_MASTERDBREADY',
    'SLEEP_MASTERMDREADY','SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP',
    'SLEEP_SYSTEMTASK','SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT',
    'SP_SERVER_DIAGNOSTICS_SLEEP','SQLTRACE_BUFFER_FLUSH',
    'SQLTRACE_INCREMENTAL_FLUSH_SLEEP','WAITFOR','XE_DISPATCHER_WAIT',
    'XE_TIMER_EVENT','BROKER_EVENTHANDLER','CHECKPOINT_QUEUE',
    'DBMIRROR_EVENTS_QUEUE','SQLTRACE_WAIT_ENTRIES','WAIT_XTP_OFFLINE_CKPT_NEW_LOG'
)
  AND wait_time_ms > 0
ORDER BY wait_time_ms DESC
"@
$waitStatsData = Invoke-SqlQuery -ConnectionString $masterConnStr -Query $waitStatsSql

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 – Currently Blocked / Long-Running Sessions
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Collecting active session data..." -ForegroundColor Cyan

$activeSessionsSql = @"
SELECT
    r.session_id                                            AS [SPID],
    r.blocking_session_id                                   AS [Blocked_By],
    r.status                                                AS [Status],
    r.wait_type                                             AS [Wait_Type],
    ROUND(r.total_elapsed_time / 1000.0, 1)               AS [Elapsed_sec],
    ROUND(r.cpu_time           / 1000.0, 1)               AS [CPU_sec],
    r.logical_reads                                         AS [Logical_Reads],
    DB_NAME(r.database_id)                                  AS [Database],
    s.login_name                                            AS [Login],
    s.host_name                                             AS [Host],
    s.program_name                                          AS [Program],
    LEFT(ISNULL(st.text,''),500)                            AS [SQL_Text]
FROM sys.dm_exec_requests r
JOIN sys.dm_exec_sessions  s ON s.session_id = r.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
WHERE r.session_id > 50
  AND r.session_id <> @@SPID
ORDER BY r.total_elapsed_time DESC
"@
$activeSessionsData = Invoke-SqlQuery -ConnectionString $masterConnStr -Query $activeSessionsSql

# ─────────────────────────────────────────────────────────────────────────────
# BUILD HTML REPORT
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[$(Get-Date -f 'HH:mm:ss')] Building HTML report..." -ForegroundColor Cyan

$timestamp   = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$reportFile  = Join-Path $OutputPath "SQLPerfAnalysis_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"
$svr         = $serverInfo.Rows[0]['ServerName']
$ver         = $serverInfo.Rows[0]['Version']
$edi         = $serverInfo.Rows[0]['Edition']

# Convert each section
$htmlPlanCacheCPU     = ConvertTo-HtmlTable -DataTable $planCacheData    -EmptyMessage "No high-CPU queries found in plan cache for this window."
$htmlPlanCacheIO      = ConvertTo-HtmlTable -DataTable $planCacheIOData  -EmptyMessage "No high-IO queries found in plan cache for this window."
$htmlMissingIndex     = ConvertTo-HtmlTable -DataTable $missingIndexData -EmptyMessage "No missing index recommendations found."
$htmlUnusedIndex      = ConvertTo-HtmlTable -DataTable $unusedIndexData  -EmptyMessage "No unused indexes found."
$htmlDupeIndex        = ConvertTo-HtmlTable -DataTable $duplicateIndexData -EmptyMessage "No duplicate indexes detected."
$htmlFragmentation    = ConvertTo-HtmlTable -DataTable $fragmentationData -EmptyMessage "No fragmented indexes above threshold."
$htmlWaitStats        = ConvertTo-HtmlTable -DataTable $waitStatsData    -EmptyMessage "No significant waits found."
$htmlActiveSessions   = ConvertTo-HtmlTable -DataTable $activeSessionsData -EmptyMessage "No active user requests at collection time."

$qsSections = ""
if ($hasQS) {
    $htmlQsCpu   = ConvertTo-HtmlTable -DataTable $qsTopCpuData           -EmptyMessage "No Query Store CPU data in window."
    $htmlQsIO    = ConvertTo-HtmlTable -DataTable $qsTopIOData            -EmptyMessage "No Query Store IO data in window."
    $htmlQsPlan  = ConvertTo-HtmlTable -DataTable $qsPlanInstabilityData  -EmptyMessage "No plan instability detected."
    $qsSections  = @"
    <section id='qs'>
      <h2>&#128269; Query Store Analysis</h2>
      <div class='note'>Query Store is <strong>ENABLED</strong> on this database — data below reflects persisted statistics.</div>

      <h3>Top $TopN Queries by CPU (Query Store)</h3>
      $htmlQsCpu

      <h3>Top $TopN Queries by Logical Reads (Query Store)</h3>
      $htmlQsIO

      <h3>Plan Instability – Queries with Multiple Plans (Possible Parameter Sniffing)</h3>
      <div class='note'>Queries with &gt; 3 distinct plans may suffer from parameter sniffing. Consider OPTIMIZE FOR UNKNOWN, query hints, or plan guides.</div>
      $htmlQsPlan
    </section>
"@
} else {
    $qsSections = @"
    <section id='qs'>
      <h2>&#128269; Query Store Analysis</h2>
      <div class='warn'>Query Store is <strong>DISABLED</strong> on this database. Enable it for deeper historical analysis:<br>
      <code>ALTER DATABASE [$DatabaseName] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);</code></div>
    </section>
"@
}

$html = @"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SQL Server Performance Report – $DatabaseName</title>
<style>
  :root{--accent:#0072C6;--warn:#C8501A;--ok:#217346;--bg:#F4F6FA;--card:#FFFFFF;--border:#D0D7E3;}
  *{box-sizing:border-box;margin:0;padding:0;}
  body{font-family:'Segoe UI',Arial,sans-serif;font-size:13px;background:var(--bg);color:#222;}
  header{background:var(--accent);color:#fff;padding:18px 28px;}
  header h1{font-size:1.5rem;}
  header p{font-size:.85rem;opacity:.85;margin-top:4px;}
  nav{background:#fff;border-bottom:1px solid var(--border);padding:10px 28px;display:flex;gap:14px;flex-wrap:wrap;}
  nav a{color:var(--accent);text-decoration:none;font-size:.82rem;font-weight:600;}
  nav a:hover{text-decoration:underline;}
  main{max-width:1600px;margin:0 auto;padding:20px 28px;}
  section{background:var(--card);border:1px solid var(--border);border-radius:6px;margin-bottom:24px;padding:20px 24px;}
  h2{font-size:1.1rem;color:var(--accent);margin-bottom:14px;border-bottom:2px solid var(--accent);padding-bottom:6px;}
  h3{font-size:.9rem;margin:18px 0 8px;color:#333;}
  table{width:100%;border-collapse:collapse;font-size:.78rem;margin-bottom:8px;}
  th{background:var(--accent);color:#fff;padding:7px 10px;text-align:left;white-space:nowrap;}
  td{padding:6px 10px;border-bottom:1px solid #E8ECF3;vertical-align:top;word-break:break-word;max-width:600px;}
  tr:nth-child(even) td{background:#F8FAFD;}
  tr:hover td{background:#EDF3FB;}
  .empty{color:#888;font-style:italic;padding:6px 0;}
  .note{background:#EDF4FF;border-left:4px solid var(--accent);padding:8px 12px;margin-bottom:12px;font-size:.82rem;border-radius:3px;}
  .warn{background:#FFF3EE;border-left:4px solid var(--warn);padding:8px 12px;margin-bottom:12px;font-size:.82rem;border-radius:3px;}
  .meta-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;margin-bottom:8px;}
  .meta-card{background:#F0F4FF;border:1px solid var(--border);border-radius:5px;padding:10px 14px;}
  .meta-card .label{font-size:.72rem;text-transform:uppercase;color:#666;letter-spacing:.5px;}
  .meta-card .value{font-size:.95rem;font-weight:700;color:#111;margin-top:2px;}
  code{background:#EEF;padding:2px 5px;border-radius:3px;font-size:.8rem;}
  footer{text-align:center;color:#999;font-size:.75rem;padding:20px 0 40px;}
</style>
</head>
<body>
<header>
  <h1>&#9889; SQL Server Performance Analysis Report</h1>
  <p>Server: <strong>$svr</strong> &nbsp;|&nbsp; Database: <strong>$DatabaseName</strong> &nbsp;|&nbsp; Generated: <strong>$timestamp</strong> &nbsp;|&nbsp; Window: <strong>$windowDesc</strong></p>
</header>

<nav>
  <a href='#overview'>Overview</a>
  <a href='#plancache'>Plan Cache</a>
  <a href='#qs'>Query Store</a>
  <a href='#missing'>Missing Indexes</a>
  <a href='#unused'>Unused Indexes</a>
  <a href='#dupe'>Duplicate Indexes</a>
  <a href='#frag'>Fragmentation</a>
  <a href='#waits'>Wait Stats</a>
  <a href='#sessions'>Active Sessions</a>
</nav>

<main>

  <!-- OVERVIEW -->
  <section id='overview'>
    <h2>&#128203; Instance Overview</h2>
    <div class='meta-grid'>
      <div class='meta-card'><div class='label'>Server</div><div class='value'>$svr</div></div>
      <div class='meta-card'><div class='label'>Version</div><div class='value'>$ver</div></div>
      <div class='meta-card'><div class='label'>Edition</div><div class='value'>$edi</div></div>
      <div class='meta-card'><div class='label'>Database</div><div class='value'>$DatabaseName</div></div>
      <div class='meta-card'><div class='label'>Window Start</div><div class='value'>$($StartTime.ToString('yyyy-MM-dd HH:mm'))</div></div>
      <div class='meta-card'><div class='label'>Window End</div><div class='value'>$($EndTime.ToString('yyyy-MM-dd HH:mm'))</div></div>
      <div class='meta-card'><div class='label'>Window Duration</div><div class='value'>$windowMins minutes</div></div>
      <div class='meta-card'><div class='label'>Query Store</div><div class='value'>$(if ($hasQS){'ENABLED'}else{'DISABLED'})</div></div>
      <div class='meta-card'><div class='label'>Top N per Section</div><div class='value'>$TopN</div></div>
      <div class='meta-card'><div class='label'>Auth Method</div><div class='value'>Windows (Integrated)</div></div>
    </div>
    <div class='note'><strong>How to use this report:</strong> Start with <em>Wait Stats</em> to identify the dominant bottleneck, then review <em>Missing Indexes</em> and <em>Plan Cache / Query Store</em> for the worst offending queries. Use the DDL snippets in <em>Fragmentation</em> and <em>Unused Indexes</em> sections for maintenance actions.</div>
  </section>

  <!-- PLAN CACHE -->
  <section id='plancache'>
    <h2>&#128293; Plan Cache – Problem Queries</h2>
    <div class='note'>Data sourced from <code>sys.dm_exec_query_stats</code>. Plan cache can be cleared by restarts or memory pressure — use Query Store for persistent data.</div>

    <h3>Top $TopN by Total CPU Time</h3>
    $htmlPlanCacheCPU

    <h3>Top $TopN by Average Logical Reads (I/O Pressure)</h3>
    $htmlPlanCacheIO
  </section>

  <!-- QUERY STORE -->
  $qsSections

  <!-- MISSING INDEXES -->
  <section id='missing'>
    <h2>&#128161; Missing Index Recommendations</h2>
    <div class='note'>Ranked by <strong>Improvement Score</strong> = <em>avg_total_user_cost × avg_user_impact × (seeks + scans)</em>. Focus on the top items — do not blindly create all suggestions. Validate with actual query plans before implementing. The <em>Create_Index_DDL</em> column contains ready-to-use T-SQL — rename the index before running.</div>
    $htmlMissingIndex
  </section>

  <!-- UNUSED INDEXES -->
  <section id='unused'>
    <h2>&#128465; Unused Indexes (Maintenance Overhead, Zero Read Benefit)</h2>
    <div class='warn'><strong>Caution:</strong> Stats reset on service restart. Only drop indexes after confirming they are unused across a representative workload period. Check application code and any scheduled jobs that may only run monthly/quarterly. Always script the index before dropping.</div>
    $htmlUnusedIndex
  </section>

  <!-- DUPLICATE INDEXES -->
  <section id='dupe'>
    <h2>&#128257; Duplicate / Redundant Indexes</h2>
    <div class='note'>These index pairs share identical key columns. The one with fewer included columns or lower usage is the candidate for removal. Confirm with <code>sys.dm_db_index_usage_stats</code> before dropping.</div>
    $htmlDupeIndex
  </section>

  <!-- FRAGMENTATION -->
  <section id='frag'>
    <h2>&#128295; Index Fragmentation</h2>
    <div class='note'>Only indexes &ge; <strong>$MinIndexSizePages pages</strong> with fragmentation &ge; <strong>$FragmentationThresholdPct%</strong> are shown. Rule of thumb: 10–30% → REORGANIZE; &gt;30% → REBUILD. Schedule maintenance during off-peak hours. <code>ONLINE = ON</code> requires Enterprise edition.</div>
    $htmlFragmentation
  </section>

  <!-- WAIT STATS -->
  <section id='waits'>
    <h2>&#9203; Top Wait Statistics (Cumulative Since Last Restart)</h2>
    <div class='note'>Common bottlenecks: <strong>PAGEIOLATCH_*</strong> = I/O / missing indexes; <strong>LCK_M_*</strong> = blocking / lock contention; <strong>CXPACKET / CXCONSUMER</strong> = parallelism tuning needed; <strong>WRITELOG</strong> = transaction log I/O; <strong>RESOURCE_SEMAPHORE</strong> = memory grants.</div>
    $htmlWaitStats
  </section>

  <!-- ACTIVE SESSIONS -->
  <section id='sessions'>
    <h2>&#128100; Active Sessions at Collection Time</h2>
    <div class='note'>A non-zero <strong>Blocked_By</strong> value indicates the session is waiting on another. Follow the blocking chain to find the head blocker.</div>
    $htmlActiveSessions
  </section>

</main>
<footer>Report generated by Invoke-SQLPerfAnalysis.ps1 &nbsp;|&nbsp; $timestamp &nbsp;|&nbsp; Windows Auth (Integrated Security)</footer>
</body>
</html>
"@

$html | Out-File -FilePath $reportFile -Encoding UTF8

Write-Host "`n[$(Get-Date -f 'HH:mm:ss')] ✅ Report written to: $reportFile" -ForegroundColor Green
Write-Host ""

# ─────────────────────────────────────────────────────────────────────────────
# CONSOLE SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "══════════════ QUICK SUMMARY ══════════════" -ForegroundColor Cyan
Write-Host ("  Missing index recommendations : {0,5}" -f $missingIndexData.Rows.Count)
Write-Host ("  Unused indexes found          : {0,5}" -f $unusedIndexData.Rows.Count)
Write-Host ("  Duplicate index pairs         : {0,5}" -f $duplicateIndexData.Rows.Count)
Write-Host ("  Fragmented indexes flagged    : {0,5}" -f $fragmentationData.Rows.Count)
Write-Host ("  Active sessions at scan time  : {0,5}" -f $activeSessionsData.Rows.Count)
if ($waitStatsData.Rows.Count -gt 0) {
    $topWait = $waitStatsData.Rows[0]['Wait_Type']
    Write-Host ("  Top wait type                 : {0}" -f $topWait)
}
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Offer to open the report
if ($env:OS -match "Windows") {
    $open = Read-Host "Open the HTML report in your browser now? [Y/N]"
    if ($open -match '^[Yy]') {
        Start-Process $reportFile
    }
}
