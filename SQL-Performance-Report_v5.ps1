#Requires -Version 5.1
<#
.SYNOPSIS
    SQL Server Comprehensive Performance Report Generator

.DESCRIPTION
    Generates a detailed HTML performance report for all SQL Server databases
    including query analysis, wait statistics, index health, blocking, resource
    usage, and actionable recommendations — with interactive Chart.js graphs.

.PARAMETER ServerInstance
    SQL Server instance name (e.g. "SQLSERVER01" or "SQLSERVER01\INSTANCE")

.PARAMETER ReportPath
    Output path for the HTML report. Defaults to current directory.

.PARAMETER StartTime
    Start of the analysis time window. Defaults to 24 hours ago.

.PARAMETER EndTime
    End of the analysis time window. Defaults to now.

.PARAMETER TopN
    Number of top queries/objects to include in each section. Default: 25

.PARAMETER SqlCredential
    PSCredential for SQL Authentication. If omitted, Windows Authentication is used.

.PARAMETER ExcludeDatabases
    Comma-separated list of database names to exclude (e.g. "tempdb,model").

.EXAMPLE
    .\SQL-Performance-Report.ps1 -ServerInstance "SQLSERVER01" -StartTime (Get-Date).AddDays(-7)

.EXAMPLE
    .\SQL-Performance-Report.ps1 -ServerInstance "SQLSERVER01\DEV" `
        -StartTime "2025-05-01" -EndTime "2025-05-12" `
        -TopN 30 -ReportPath "C:\Reports"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ServerInstance,

    [Parameter(Mandatory = $false)]
    [string]$ReportPath = (Get-Location).Path,

    [Parameter(Mandatory = $false)]
    [datetime]$StartTime = (Get-Date).AddHours(-24),

    [Parameter(Mandatory = $false)]
    [datetime]$EndTime = (Get-Date),

    [Parameter(Mandatory = $false)]
    [int]$TopN = 25,

    [Parameter(Mandatory = $false)]
    [System.Management.Automation.PSCredential]$SqlCredential,

    [Parameter(Mandatory = $false)]
    [string[]]$ExcludeDatabases = @('tempdb', 'model', 'msdb', 'master', 'resource', 'distribution')
)

# StrictMode -Version Latest is intentionally NOT set:
# it throws PropertyNotFoundStrict on DataTable.Rows from .NET interop
# even when the property is valid — causing false failures in reporting scripts.
$ErrorActionPreference = 'Continue'   # queries that fail are logged as warnings, not crashes

#region ── Helpers ─────────────────────────────────────────────────────────────

function Write-Log {
    param([string]$Message, [string]$Level = 'INFO')
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $color = switch ($Level) { 'WARN' { 'Yellow' } 'ERROR' { 'Red' } 'OK' { 'Green' } default { 'Cyan' } }
    Write-Host "[$ts] [$Level] $Message" -ForegroundColor $color
}

function New-EmptyTable { return (New-Object System.Data.DataTable) }

function Invoke-SqlQuery {
    param(
        [string]$Query,
        [string]$Database = 'master',
        [int]$TimeoutSeconds = 120
    )
    try {
        $connStr = "Server=$ServerInstance;Database=$Database;Connect Timeout=30;"
        if ($SqlCredential) {
            $connStr += "User Id=$($SqlCredential.UserName);Password=$($SqlCredential.GetNetworkCredential().Password);"
        } else {
            $connStr += "Integrated Security=True;"
        }
        $conn   = New-Object System.Data.SqlClient.SqlConnection($connStr)
        $conn.Open()
        $cmd    = $conn.CreateCommand()
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $TimeoutSeconds
        # Fill directly into a DataTable (not DataSet) so PowerShell never
        # enumerates / unwraps it into DataRow[] when the function returns.
        $dt      = New-Object System.Data.DataTable
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
        [void]$adapter.Fill($dt)
        $conn.Close()
        return $dt          # always a DataTable — never null, never DataRow[]
    } catch {
        Write-Log "Query failed on [$Database]: $_" 'WARN'
        return New-EmptyTable
    }
}

function ConvertTo-HtmlTable {
    # No strict type on $Table — PowerShell sometimes unwraps DataTable into
    # DataRow[] or a single DataRow when returning from functions.
    # We normalise whatever arrives back into rows + columns here.
    param(
        $Table,
        [string]$CssClass = 'data-table',
        [string]$EmptyMessage = 'No data available.'
    )

    # ── Normalise input ──────────────────────────────────────────────────
    $rows    = $null
    $columns = $null

    if ($Table -is [System.Data.DataTable]) {
        $rows    = $Table.Rows
        $columns = $Table.Columns
    }
    elseif ($Table -is [System.Data.DataRow]) {
        # Single-row result — wrap in array; borrow column list from parent table
        $rows    = @($Table)
        $columns = $Table.Table.Columns
    }
    elseif ($Table -is [System.Object[]] -or $Table -is [System.Array]) {
        # Array of DataRows (enumerated DataTable)
        if ($Table.Count -gt 0 -and $Table[0] -is [System.Data.DataRow]) {
            $rows    = $Table
            $columns = $Table[0].Table.Columns
        }
    }

    if ($null -eq $rows -or $rows.Count -eq 0) {
        return "<p class='no-data'>$EmptyMessage</p>"
    }

    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.Append("<div class='table-wrapper'><table class='$CssClass'><thead><tr>")
    foreach ($col in $columns) {
        [void]$sb.Append("<th>$([System.Web.HttpUtility]::HtmlEncode($col.ColumnName))</th>")
    }
    [void]$sb.Append("</tr></thead><tbody>")
    foreach ($row in $rows) {
        [void]$sb.Append("<tr>")
        foreach ($col in $columns) {
            $val = if ($row[$col.ColumnName] -eq [System.DBNull]::Value) { '' } else { $row[$col.ColumnName].ToString() }
            [void]$sb.Append("<td>$([System.Web.HttpUtility]::HtmlEncode($val))</td>")
        }
        [void]$sb.Append("</tr>")
    }
    [void]$sb.Append("</tbody></table></div>")
    return $sb.ToString()
}

function Get-SeverityBadge {
    param([string]$Level)
    $map = @{ Critical = 'badge-critical'; High = 'badge-high'; Medium = 'badge-medium'; Low = 'badge-low'; Good = 'badge-good' }
    $cls = if ($map.ContainsKey($Level)) { $map[$Level] } else { 'badge-low' }
    return "<span class='badge $cls'>$Level</span>"
}

Add-Type -AssemblyName System.Web

#endregion

#region ── Data Collection ─────────────────────────────────────────────────────

Write-Log "=== SQL Server Performance Report ===" 'OK'
Write-Log "Server  : $ServerInstance"
Write-Log "Window  : $($StartTime.ToString('yyyy-MM-dd HH:mm')) → $($EndTime.ToString('yyyy-MM-dd HH:mm'))"
Write-Log "Output  : $ReportPath"

# ── Server Properties ──────────────────────────────────────────────────────────
Write-Log "Collecting server properties..."
$qServerInfo = @"
SELECT
    SERVERPROPERTY('ServerName')           AS ServerName,
    SERVERPROPERTY('ProductVersion')       AS ProductVersion,
    SERVERPROPERTY('ProductLevel')         AS ProductLevel,
    SERVERPROPERTY('Edition')              AS Edition,
    SERVERPROPERTY('EngineEdition')        AS EngineEdition,
    SERVERPROPERTY('Collation')            AS Collation,
    SERVERPROPERTY('IsClustered')          AS IsClustered,
    SERVERPROPERTY('IsHadrEnabled')        AS IsHadrEnabled,
    cpu_count                              AS LogicalCPUs,
    hyperthread_ratio                      AS HyperthreadRatio,
    physical_memory_kb / 1024             AS PhysicalMemoryMB,
    committed_kb / 1024                   AS CommittedMemoryMB,
    committed_target_kb / 1024            AS TargetMemoryMB,
    sqlserver_start_time                   AS SQLStartTime
FROM sys.dm_os_sys_info;
"@
$serverInfo = Invoke-SqlQuery -Query $qServerInfo

# ── Database List ──────────────────────────────────────────────────────────────
Write-Log "Enumerating databases..."
$excludeList = ($ExcludeDatabases | ForEach-Object { "'$_'" }) -join ','
$qDatabases = @"
SELECT
    d.name                                          AS DatabaseName,
    d.state_desc                                    AS State,
    d.recovery_model_desc                           AS RecoveryModel,
    d.compatibility_level                           AS CompatibilityLevel,
    d.collation_name                                AS Collation,
    d.is_auto_shrink_on                             AS AutoShrink,
    d.is_auto_update_stats_on                       AS AutoUpdateStats,
    d.is_read_only                                  AS IsReadOnly,
    CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(18,2)) AS TotalSizeMB,
    d.log_reuse_wait_desc                           AS LogReuseWait,
    d.page_verify_option_desc                       AS PageVerify,
    d.user_access_desc                              AS UserAccess
FROM sys.databases d
JOIN sys.master_files mf ON d.database_id = mf.database_id
WHERE d.name NOT IN ($excludeList)
  AND d.state = 0
GROUP BY d.name, d.state_desc, d.recovery_model_desc, d.compatibility_level,
         d.collation_name, d.is_auto_shrink_on, d.is_auto_update_stats_on,
         d.is_read_only, d.log_reuse_wait_desc, d.page_verify_option_desc,
         d.user_access_desc
ORDER BY TotalSizeMB DESC;
"@
$databases = Invoke-SqlQuery -Query $qDatabases

# ── Wait Statistics ────────────────────────────────────────────────────────────
Write-Log "Collecting wait statistics..."
$qWaits = @"
WITH Waits AS (
    SELECT
        wait_type,
        wait_time_ms / 1000.0                          AS WaitSeconds,
        (wait_time_ms - signal_wait_time_ms) / 1000.0  AS ResourceWaitSeconds,
        signal_wait_time_ms / 1000.0                    AS SignalWaitSeconds,
        waiting_tasks_count                             AS WaitCount,
        100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER (), 0) AS Pct
    FROM sys.dm_os_wait_stats
    WHERE wait_type NOT IN (
        'SLEEP_TASK','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_AUTO_EVENT',
        'DISPATCHER_QUEUE_SEMAPHORE','FT_IFTS_SCHEDULER_IDLE_WAIT',
        'HADR_FILESTREAM_IOMGR_IOCOMPLETION','HADR_WORK_QUEUE',
        'HADR_CLUSAPI_CALL','HADR_TIMER_TASK','HADR_TRANSPORT_DEINIT_TASK',
        'HADR_TRANSPORT_THREAD','KSOURCE_WAKEUP','LAZYWRITER_SLEEP',
        'LOGMGR_QUEUE','ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH',
        'RESOURCE_QUEUE','SERVER_IDLE_CHECK','SLEEP_DBSTARTUP',
        'SLEEP_DBRECOVER','SLEEP_DBTASK','SLEEP_MASTERDBREADY',
        'SLEEP_MASTERMDREADY','SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP',
        'SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT','SP_SERVER_DIAGNOSTICS_SLEEP',
        'SQLTRACE_BUFFER_FLUSH','SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        'WAIT_XTP_OFFLINE_CKPT_NEW_LOG','WAITFOR','XE_DISPATCHER_WAIT',
        'XE_TIMER_EVENT','BROKER_EVENTHANDLER','CHECKPOINT_QUEUE',
        'DBMIRROR_EVENTS_QUEUE','SQLTRACE_WAIT_ENTRIES',
        'WAIT_XTP_CHECKPOINT_RECOVERY','DIRTY_PAGE_POLL',
        'HADR_LOGCAPTURE_WAIT','HADR_NOTIFICATION_DEQUEUE',
        'HADR_RECEIVE_IS_XACT_SENDER','HADR_SYNC_COMMIT',
        'WAIT_HADR_WORKITEM_COMPLETION','PREEMPTIVE_OS_OPERATIONSINFO'
    )
    AND wait_time_ms > 0
)
SELECT TOP 20
    wait_type                               AS WaitType,
    CAST(WaitSeconds AS DECIMAL(18,2))      AS TotalWaitSec,
    CAST(ResourceWaitSeconds AS DECIMAL(18,2)) AS ResourceWaitSec,
    CAST(SignalWaitSeconds AS DECIMAL(18,2))   AS SignalWaitSec,
    WaitCount,
    CAST(WaitSeconds / NULLIF(WaitCount,0) AS DECIMAL(18,4)) AS AvgWaitSec,
    CAST(Pct AS DECIMAL(5,2))               AS PctTotal
FROM Waits
ORDER BY WaitSeconds DESC;
"@
$waitStats = Invoke-SqlQuery -Query $qWaits

# ── Top CPU Queries ────────────────────────────────────────────────────────────
Write-Log "Collecting top CPU queries..."
$qTopCPU = @"
SELECT TOP $TopN
    qs.total_worker_time / 1000              AS TotalCPU_ms,
    qs.execution_count                       AS Executions,
    CAST(qs.total_worker_time / 1000.0 / NULLIF(qs.execution_count,0) AS DECIMAL(18,2)) AS AvgCPU_ms,
    qs.total_logical_reads                   AS TotalLogicalReads,
    CAST(qs.total_logical_reads * 1.0 / NULLIF(qs.execution_count,0) AS DECIMAL(18,0)) AS AvgLogicalReads,
    qs.total_elapsed_time / 1000             AS TotalDuration_ms,
    CAST(qs.total_elapsed_time / 1000.0 / NULLIF(qs.execution_count,0) AS DECIMAL(18,2)) AS AvgDuration_ms,
    qs.total_logical_writes                  AS TotalLogicalWrites,
    qs.plan_generation_num                   AS PlanGenerations,
    DB_NAME(qp.dbid)                         AS DatabaseName,
    OBJECT_NAME(qp.objectid, qp.dbid)        AS ObjectName,
    qs.creation_time                         AS PlanCreatedAt,
    qs.last_execution_time                   AS LastExecuted,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1) AS StatementText
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE qs.last_execution_time BETWEEN '$($StartTime.ToString("yyyy-MM-dd HH:mm:ss"))' AND '$($EndTime.ToString("yyyy-MM-dd HH:mm:ss"))'
ORDER BY qs.total_worker_time DESC;
"@
$topCPU = Invoke-SqlQuery -Query $qTopCPU

# ── Top I/O Queries ────────────────────────────────────────────────────────────
Write-Log "Collecting top I/O queries..."
$qTopIO = @"
SELECT TOP $TopN
    qs.total_logical_reads + qs.total_logical_writes  AS TotalIO,
    qs.total_logical_reads                            AS TotalLogicalReads,
    qs.total_logical_writes                           AS TotalLogicalWrites,
    qs.total_physical_reads                           AS TotalPhysicalReads,
    qs.execution_count                                AS Executions,
    CAST((qs.total_logical_reads + qs.total_logical_writes) * 1.0
         / NULLIF(qs.execution_count,0) AS DECIMAL(18,0))  AS AvgIO,
    qs.total_worker_time / 1000                       AS TotalCPU_ms,
    DB_NAME(qp.dbid)                                  AS DatabaseName,
    OBJECT_NAME(qp.objectid, qp.dbid)                 AS ObjectName,
    qs.last_execution_time                            AS LastExecuted,
    SUBSTRING(qt.text,(qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1) AS StatementText
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE qs.last_execution_time BETWEEN '$($StartTime.ToString("yyyy-MM-dd HH:mm:ss"))' AND '$($EndTime.ToString("yyyy-MM-dd HH:mm:ss"))'
ORDER BY TotalIO DESC;
"@
$topIO = Invoke-SqlQuery -Query $qTopIO

# ── Top Execution Count Queries ────────────────────────────────────────────────
Write-Log "Collecting most executed queries..."
$qTopExec = @"
SELECT TOP $TopN
    qs.execution_count                       AS Executions,
    qs.total_worker_time / 1000              AS TotalCPU_ms,
    CAST(qs.total_worker_time / 1000.0 / NULLIF(qs.execution_count,0) AS DECIMAL(18,2)) AS AvgCPU_ms,
    qs.total_elapsed_time / 1000             AS TotalDuration_ms,
    CAST(qs.total_elapsed_time / 1000.0 / NULLIF(qs.execution_count,0) AS DECIMAL(18,2)) AS AvgDuration_ms,
    qs.total_logical_reads                   AS TotalLogicalReads,
    DB_NAME(qp.dbid)                         AS DatabaseName,
    OBJECT_NAME(qp.objectid, qp.dbid)        AS ObjectName,
    qs.last_execution_time                   AS LastExecuted,
    SUBSTRING(qt.text,(qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1) AS StatementText
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE qs.last_execution_time BETWEEN '$($StartTime.ToString("yyyy-MM-dd HH:mm:ss"))' AND '$($EndTime.ToString("yyyy-MM-dd HH:mm:ss"))'
ORDER BY qs.execution_count DESC;
"@
$topExec = Invoke-SqlQuery -Query $qTopExec

# ── Missing Indexes ────────────────────────────────────────────────────────────
Write-Log "Collecting missing index recommendations..."
$qMissingIdx = @"
SELECT TOP $TopN
    CAST(migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) AS DECIMAL(18,2))
                                        AS ImpactScore,
    CAST(migs.avg_user_impact AS DECIMAL(5,2)) AS AvgImpactPct,
    migs.user_seeks                     AS Seeks,
    migs.user_scans                     AS Scans,
    migs.last_user_seek                 AS LastSeek,
    DB_NAME(mid.database_id)            AS DatabaseName,
    mid.statement                       AS TableName,
    mid.equality_columns                AS EqualityColumns,
    mid.inequality_columns              AS InequalityColumns,
    mid.included_columns                AS IncludedColumns,
    'CREATE INDEX [IX_' + REPLACE(REPLACE(mid.statement,']',''),'.','_') +
        '_' + REPLACE(ISNULL(mid.equality_columns,''),'[','') +
        '] ON ' + mid.statement +
        ' (' + ISNULL(mid.equality_columns,'') +
        CASE WHEN mid.inequality_columns IS NOT NULL
             THEN CASE WHEN mid.equality_columns IS NOT NULL THEN ',' ELSE '' END
                  + mid.inequality_columns ELSE '' END + ')' +
        CASE WHEN mid.included_columns IS NOT NULL
             THEN ' INCLUDE (' + mid.included_columns + ')' ELSE '' END AS CreateIndexScript
FROM sys.dm_db_missing_index_groups mig
JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
JOIN sys.dm_db_missing_index_details mid      ON mig.index_handle       = mid.index_handle
WHERE DB_NAME(mid.database_id) NOT IN ($excludeList)
ORDER BY ImpactScore DESC;
"@
$missingIdx = Invoke-SqlQuery -Query $qMissingIdx

# ── Unused Indexes ─────────────────────────────────────────────────────────────
Write-Log "Collecting unused indexes..."
$qUnusedIdx = @"
SELECT TOP $TopN
    DB_NAME()                           AS DatabaseName,
    OBJECT_NAME(i.object_id)            AS TableName,
    i.name                              AS IndexName,
    i.type_desc                         AS IndexType,
    ius.user_seeks                      AS Seeks,
    ius.user_scans                      AS Scans,
    ius.user_lookups                    AS Lookups,
    ius.user_updates                    AS Updates,
    ius.last_user_seek                  AS LastSeek,
    ius.last_user_scan                  AS LastScan,
    ius.last_user_update                AS LastUpdate,
    8 * SUM(a.used_pages)               AS IndexSizeKB
FROM sys.indexes i
JOIN sys.objects o ON i.object_id = o.object_id
LEFT JOIN sys.dm_db_index_usage_stats ius
       ON i.object_id = ius.object_id AND i.index_id = ius.index_id
          AND ius.database_id = DB_ID()
LEFT JOIN sys.partitions p    ON i.object_id = p.object_id AND i.index_id = p.index_id
LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE o.type = 'U'
  AND i.index_id > 1          -- exclude heaps and clustered
  AND i.is_primary_key = 0
  AND i.is_unique_constraint = 0
  AND ISNULL(ius.user_seeks,0) + ISNULL(ius.user_scans,0) + ISNULL(ius.user_lookups,0) = 0
  AND ISNULL(ius.user_updates,0) > 0
GROUP BY i.object_id, i.name, i.type_desc, o.name,
         ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates,
         ius.last_user_seek, ius.last_user_scan, ius.last_user_update
ORDER BY ius.user_updates DESC;
"@
$unusedIdx = Invoke-SqlQuery -Query $qUnusedIdx

# ── Index Fragmentation ────────────────────────────────────────────────────────
Write-Log "Collecting index fragmentation (this may take a moment)..."
$qFragmentation = @"
SELECT TOP $TopN
    DB_NAME(ps.database_id)             AS DatabaseName,
    OBJECT_NAME(ps.object_id)           AS TableName,
    i.name                              AS IndexName,
    ps.index_type_desc                  AS IndexType,
    CAST(ps.avg_fragmentation_in_percent AS DECIMAL(5,2)) AS FragmentationPct,
    ps.page_count                       AS Pages,
    CAST(ps.page_count * 8.0 / 1024 AS DECIMAL(18,2))    AS IndexSizeMB,
    ps.record_count                     AS RecordCount,
    CASE
        WHEN ps.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
        WHEN ps.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
        ELSE 'NONE'
    END AS RecommendedAction
FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'LIMITED') ps
JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
WHERE ps.page_count > 100
  AND ps.avg_fragmentation_in_percent > 10
  AND DB_NAME(ps.database_id) NOT IN ($excludeList)
ORDER BY ps.avg_fragmentation_in_percent DESC;
"@
$fragmentation = Invoke-SqlQuery -Query $qFragmentation -TimeoutSeconds 600

# ── Blocking / Long Running Queries ───────────────────────────────────────────
Write-Log "Collecting current blocking and active sessions..."
$qBlocking = @"
SELECT
    r.session_id                        AS SPID,
    r.blocking_session_id               AS BlockedBy,
    DB_NAME(r.database_id)              AS DatabaseName,
    r.status                            AS Status,
    r.command                           AS Command,
    r.wait_type                         AS WaitType,
    r.wait_time / 1000                  AS WaitSec,
    r.cpu_time / 1000                   AS CPUSec,
    r.total_elapsed_time / 1000         AS ElapsedSec,
    r.logical_reads                     AS LogicalReads,
    r.writes                            AS Writes,
    r.[row_count]                       AS RowsAffected,
    s.login_name                        AS LoginName,
    s.host_name                         AS HostName,
    s.program_name                      AS ProgramName,
    SUBSTRING(qt.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS StatementText
FROM sys.dm_exec_requests r
JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) qt
WHERE r.session_id <> @@SPID
ORDER BY r.blocking_session_id DESC, r.total_elapsed_time DESC;
"@
$blocking = Invoke-SqlQuery -Query $qBlocking

# ── Memory Usage ───────────────────────────────────────────────────────────────
Write-Log "Collecting memory stats..."
$qMemory = @"
SELECT
    physical_memory_in_use_kb / 1024    AS SQL_Physical_MB,
    locked_page_allocations_kb / 1024   AS Locked_Pages_MB,
    virtual_address_space_committed_kb / 1024 AS VAS_Committed_MB,
    memory_utilization_percentage       AS Memory_Utilization_Pct,
    page_fault_count                    AS Page_Fault_Count
FROM sys.dm_os_process_memory;
"@
$memoryInfo = Invoke-SqlQuery -Query $qMemory

$qBufferPool = @"
SELECT
    COUNT(*) * 8 / 1024                             AS TotalBufferMB,
    SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) * 8 / 1024 AS DirtyPagesMB,
    SUM(CASE WHEN is_modified = 0 THEN 1 ELSE 0 END) * 8 / 1024 AS CleanPagesMB
FROM sys.dm_os_buffer_descriptors
WHERE database_id <> 32767;
"@
$bufferPool = Invoke-SqlQuery -Query $qBufferPool

$qBufferByDB = @"
SELECT
    DB_NAME(database_id)                AS DatabaseName,
    COUNT(*) * 8 / 1024                 AS BufferSizeMB,
    COUNT(*) * 8 * 100.0 /
        NULLIF((SELECT COUNT(*)*8 FROM sys.dm_os_buffer_descriptors WHERE database_id <> 32767),0) AS PctOfBuffer
FROM sys.dm_os_buffer_descriptors
WHERE database_id <> 32767
GROUP BY database_id
ORDER BY BufferSizeMB DESC;
"@
$bufferByDB = Invoke-SqlQuery -Query $qBufferByDB

# ── I/O Stats by Database File ─────────────────────────────────────────────────
Write-Log "Collecting I/O stats..."
$qIOStats = @"
SELECT
    DB_NAME(vfs.database_id)            AS DatabaseName,
    mf.physical_name                    AS FileName,
    mf.type_desc                        AS FileType,
    vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads,0)  AS AvgReadLatency_ms,
    vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes,0) AS AvgWriteLatency_ms,
    (vfs.io_stall) / NULLIF(vfs.num_of_reads + vfs.num_of_writes,0) AS AvgLatency_ms,
    vfs.num_of_reads                    AS TotalReads,
    vfs.num_of_writes                   AS TotalWrites,
    vfs.num_of_bytes_read / 1048576     AS TotalReadMB,
    vfs.num_of_bytes_written / 1048576  AS TotalWrittenMB,
    CAST(mf.size * 8.0 / 1024 AS DECIMAL(18,2)) AS FileSizeMB
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
JOIN sys.master_files mf ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
WHERE DB_NAME(vfs.database_id) NOT IN ($excludeList)
ORDER BY AvgLatency_ms DESC;
"@
$ioStats = Invoke-SqlQuery -Query $qIOStats

# ── Temp DB usage ──────────────────────────────────────────────────────────────
Write-Log "Collecting TempDB usage..."
$qTempDB = @"
SELECT TOP 20
    t.session_id                        AS SPID,
    DB_NAME(r.database_id)              AS DatabaseName,
    s.login_name                        AS LoginName,
    s.host_name                         AS HostName,
    t.internal_objects_alloc_page_count * 8   AS InternalObjectsKB,
    t.user_objects_alloc_page_count * 8       AS UserObjectsKB,
    (t.internal_objects_alloc_page_count + t.user_objects_alloc_page_count) * 8 AS TotalTempDB_KB,
    r.total_elapsed_time / 1000         AS ElapsedSec,
    SUBSTRING(qt.text,(r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS StatementText
FROM sys.dm_db_session_space_usage t
JOIN sys.dm_exec_sessions s ON t.session_id = s.session_id
LEFT JOIN sys.dm_exec_requests r ON t.session_id = r.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) qt
WHERE t.internal_objects_alloc_page_count + t.user_objects_alloc_page_count > 0
ORDER BY (t.internal_objects_alloc_page_count + t.user_objects_alloc_page_count) DESC;
"@
$tempDBUsage = Invoke-SqlQuery -Query $qTempDB

# ── CPU Scheduler ──────────────────────────────────────────────────────────────
Write-Log "Collecting scheduler info..."
$qScheduler = @"
SELECT
    scheduler_id,
    cpu_id,
    status,
    is_online,
    current_tasks_count         AS CurrentTasks,
    runnable_tasks_count        AS RunnableTasks,
    current_workers_count       AS CurrentWorkers,
    active_workers_count        AS ActiveWorkers,
    work_queue_count            AS WorkQueueDepth,
    pending_disk_io_count       AS PendingDiskIO,
    load_factor                 AS LoadFactor
FROM sys.dm_os_schedulers
WHERE status = 'VISIBLE ONLINE'
ORDER BY scheduler_id;
"@
$schedulers = Invoke-SqlQuery -Query $qScheduler

# ── Database Statistics Freshness ──────────────────────────────────────────────
Write-Log "Collecting statistics freshness..."
$qStats = @"
SELECT TOP $TopN
    DB_NAME()                           AS DatabaseName,
    OBJECT_NAME(s.object_id)            AS TableName,
    s.name                              AS StatisticName,
    sp.last_updated                     AS LastUpdated,
    sp.rows                             AS TableRows,
    sp.rows_sampled                     AS RowsSampled,
    CAST(sp.rows_sampled * 100.0 / NULLIF(sp.rows,0) AS DECIMAL(5,2)) AS SampleRatePct,
    sp.modification_counter             AS ModificationsSinceUpdate
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE OBJECT_NAME(s.object_id) IS NOT NULL
  AND sp.rows > 0
  AND (sp.last_updated < DATEADD(DAY,-7, GETDATE()) OR sp.modification_counter > 10000)
ORDER BY sp.modification_counter DESC;
"@
$statsInfo = Invoke-SqlQuery -Query $qStats

Write-Log "Data collection complete." 'OK'

#endregion

#region ── Build Chart Data ───────────────────────────────────────────────────

function ConvertTo-JsonArray {
    param([array]$Items)
    return ($Items | ConvertTo-Json -Compress)
}

# Wait stats chart
$waitLabels = @(); $waitValues = @()
if ($waitStats -and $waitStats.Rows.Count -gt 0) {
    $top10Waits = $waitStats.Rows | Select-Object -First 10
    $waitLabels = ConvertTo-JsonArray ($top10Waits | ForEach-Object { $_.WaitType })
    $waitValues = ConvertTo-JsonArray ($top10Waits | ForEach-Object { [double]$_.TotalWaitSec })
}

# CPU queries bar
$cpuQueryLabels = @(); $cpuQueryValues = @()
if ($topCPU -and $topCPU.Rows.Count -gt 0) {
    $top10CPU = $topCPU.Rows | Select-Object -First 10
    $cpuQueryLabels = ConvertTo-JsonArray ($top10CPU | ForEach-Object {
        $txt = $_.StatementText -replace '[\r\n\t]+',' '
        if ($txt.Length -gt 50) { $txt.Substring(0,50) + '...' } else { $txt }
    })
    $cpuQueryValues = ConvertTo-JsonArray ($top10CPU | ForEach-Object { [long]$_.TotalCPU_ms })
}

# DB sizes
$dbSizeLabels = @(); $dbSizeValues = @()
if ($databases -and $databases.Rows.Count -gt 0) {
    $dbSizeLabels = ConvertTo-JsonArray ($databases.Rows | ForEach-Object { $_.DatabaseName })
    $dbSizeValues = ConvertTo-JsonArray ($databases.Rows | ForEach-Object { [double]$_.TotalSizeMB })
}

# Buffer by DB
$bufLabels = @(); $bufValues = @()
if ($bufferByDB -and $bufferByDB.Rows.Count -gt 0) {
    $top8Buf = $bufferByDB.Rows | Select-Object -First 8
    $bufLabels = ConvertTo-JsonArray ($top8Buf | ForEach-Object { $_.DatabaseName })
    $bufValues = ConvertTo-JsonArray ($top8Buf | ForEach-Object { [long]$_.BufferSizeMB })
}

# I/O latency
$ioLabels = @(); $ioReadValues = @(); $ioWriteValues = @()
if ($ioStats -and $ioStats.Rows.Count -gt 0) {
    $top10IO = $ioStats.Rows | Select-Object -First 10
    $ioLabels      = ConvertTo-JsonArray ($top10IO | ForEach-Object { "$($_.DatabaseName) ($($_.FileType))" })
    $ioReadValues  = ConvertTo-JsonArray ($top10IO | ForEach-Object {
        if ($_.AvgReadLatency_ms -eq [System.DBNull]::Value) { 0 } else { [double]$_.AvgReadLatency_ms }
    })
    $ioWriteValues = ConvertTo-JsonArray ($top10IO | ForEach-Object {
        if ($_.AvgWriteLatency_ms -eq [System.DBNull]::Value) { 0 } else { [double]$_.AvgWriteLatency_ms }
    })
}

# Fragmentation
$fragLabels = @(); $fragValues = @()
if ($fragmentation -and $fragmentation.Rows.Count -gt 0) {
    $top10Frag = $fragmentation.Rows | Select-Object -First 10
    $fragLabels = ConvertTo-JsonArray ($top10Frag | ForEach-Object { "$($_.TableName).$($_.IndexName)" })
    $fragValues = ConvertTo-JsonArray ($top10Frag | ForEach-Object { [double]$_.FragmentationPct })
}

#endregion

#region ── Recommendations ────────────────────────────────────────────────────

$recommendations = [System.Collections.Generic.List[hashtable]]::new()

# Missing indexes
if ($missingIdx -and $missingIdx.Rows.Count -gt 0) {
    $highImpact = $missingIdx.Rows | Where-Object { [double]$_.ImpactScore -gt 100000 }
    $count = ($highImpact | Measure-Object).Count
    if ($count -gt 0) {
        $recommendations.Add(@{
            Severity    = 'Critical'
            Category    = 'Index'
            Title       = "Missing High-Impact Indexes ($count found)"
            Description = "SQL Server has identified $count missing indexes with a combined impact score exceeding 100,000. These indexes could significantly reduce I/O and CPU usage. Review the Missing Indexes section and create them after validating your workload."
        })
    }
}

# Unused indexes
if ($unusedIdx -and $unusedIdx.Rows.Count -gt 0) {
    $recommendations.Add(@{
        Severity    = 'Medium'
        Category    = 'Index'
        Title       = "Unused Indexes Consuming Resources ($($unusedIdx.Rows.Count) found)"
        Description = "There are $($unusedIdx.Rows.Count) indexes that have never been read but are being maintained on every write. Consider dropping these after verifying they are not used in disaster recovery or rarely-run reports."
    })
}

# Heavy fragmentation
if ($fragmentation -and $fragmentation.Rows.Count -gt 0) {
    $rebuild = ($fragmentation.Rows | Where-Object { $_.RecommendedAction -eq 'REBUILD' } | Measure-Object).Count
    $reorg  = ($fragmentation.Rows | Where-Object { $_.RecommendedAction -eq 'REORGANIZE' } | Measure-Object).Count
    if ($rebuild -gt 0) {
        $recommendations.Add(@{
            Severity    = 'High'
            Category    = 'Index'
            Title       = "$rebuild Indexes Require REBUILD (>30% fragmentation)"
            Description = "Heavy fragmentation causes excessive I/O and degrades query performance. Schedule an index maintenance job during off-peak hours using REBUILD WITH (ONLINE=ON) for Enterprise Edition, or REORGANIZE for Standard Edition."
        })
    }
    if ($reorg -gt 0) {
        $recommendations.Add(@{
            Severity    = 'Medium'
            Category    = 'Index'
            Title       = "$reorg Indexes Require REORGANIZE (10–30% fragmentation)"
            Description = "Moderate fragmentation detected. Run ALTER INDEX ... REORGANIZE during low-traffic windows. REORGANIZE is an online operation and has minimal impact on production."
        })
    }
}

# Wait types analysis
if ($waitStats -and $waitStats.Rows.Count -gt 0) {
    $topWait = $waitStats.Rows[0]
    $wt = $topWait.WaitType
    if ($wt -like 'LCK*') {
        $recommendations.Add(@{
            Severity    = 'Critical'
            Category    = 'Blocking'
            Title       = "Locking Waits Dominating ($wt)"
            Description = "Lock waits are the primary bottleneck. Investigate blocking chains, review transaction isolation levels (consider READ_COMMITTED_SNAPSHOT), ensure indexes support join/filter predicates, and keep transactions short."
        })
    } elseif ($wt -like 'PAGEIOLATCH*') {
        $recommendations.Add(@{
            Severity    = 'High'
            Category    = 'Memory/IO'
            Title       = "Page I/O Latch Waits — Possible Memory Pressure ($wt)"
            Description = "High PAGEIOLATCH waits indicate SQL Server is reading data from disk because the buffer pool is too small or queries are performing excessive physical reads. Consider increasing max server memory, adding missing indexes, or upgrading storage to SSD/NVMe."
        })
    } elseif ($wt -like 'CXPACKET*' -or $wt -like 'CXCONSUMER*') {
        $recommendations.Add(@{
            Severity    = 'Medium'
            Category    = 'Parallelism'
            Title       = "Excessive Parallel Query Waits ($wt)"
            Description = "High CXPACKET/CXCONSUMER waits suggest skewed parallelism. Tune MAXDOP at server or query level, adjust Cost Threshold for Parallelism (default 5 is too low — try 50), and review queries that generate large parallel plans."
        })
    } elseif ($wt -like 'WRITELOG*') {
        $recommendations.Add(@{
            Severity    = 'High'
            Category    = 'Storage'
            Title       = "Transaction Log Write Latency High ($wt)"
            Description = "WRITELOG waits indicate the transaction log is a bottleneck. Move log files to a dedicated, low-latency disk (SSD preferred), ensure no synchronous mirroring/AG lag, check for large, infrequent checkpoints (increase recovery interval)."
        })
    } elseif ($wt -like 'SOS_SCHEDULER_YIELD*') {
        $recommendations.Add(@{
            Severity    = 'High'
            Category    = 'CPU'
            Title       = "CPU Pressure — Scheduler Yield Waits ($wt)"
            Description = "Threads are voluntarily yielding the scheduler, indicating CPU saturation. Identify and optimize top CPU-consuming queries, check for parameter sniffing issues, consider adding CPUs or limiting parallelism on poorly scaling queries."
        })
    }
}

# Blocking
if ($blocking -and $blocking.Rows.Count -gt 0) {
    $blockedSessions = ($blocking.Rows | Where-Object { $_.BlockedBy -ne 0 } | Measure-Object).Count
    if ($blockedSessions -gt 0) {
        $recommendations.Add(@{
            Severity    = 'Critical'
            Category    = 'Blocking'
            Title       = "Active Blocking Detected ($blockedSessions sessions blocked)"
            Description = "There are currently $blockedSessions blocked sessions. Blocking reduces throughput and can cascade. Identify the head blocker, review long-running transactions, ensure proper indexing, and consider READ_COMMITTED_SNAPSHOT isolation level."
        })
    }
}

# TempDB pressure
if ($tempDBUsage.Rows.Count -gt 0) {
    $maxTempKB = @($tempDBUsage.Rows | ForEach-Object {
        [long]$_.TotalTempDB_KB
    } | Measure-Object -Maximum).Maximum
    if ($maxTempKB -gt 512000) {
        $recommendations.Add(@{
            Severity    = 'High'
            Category    = 'TempDB'
            Title       = "High TempDB Usage ($(($maxTempKB/1024).ToString('N0')) MB per session)"
            Description = "A session is using over 500 MB of TempDB. This suggests large sort operations, hash joins, or cursors spilling to disk. Review the query plans for spill operators, optimize joins, and ensure adequate memory grants. Consider pre-sorting or partitioning large datasets."
        })
    }
}

# Auto-shrink
if ($databases.Rows.Count -gt 0) {
    $autoShrink = ($databases.Rows | Where-Object { $_.AutoShrink -eq $true } | Measure-Object).Count
    if ($autoShrink -gt 0) {
        $recommendations.Add(@{
            Severity    = 'High'
            Category    = 'Configuration'
            Title       = "AUTO_SHRINK Enabled on $autoShrink Database(s)"
            Description = "AUTO_SHRINK causes severe fragmentation, excessive I/O, and log VLF growth. Disable it immediately with: ALTER DATABASE [dbname] SET AUTO_SHRINK OFF. Manage file sizes manually instead."
        })
    }
}

# Page verify
if ($databases.Rows.Count -gt 0) {
    $noChecksum = ($databases.Rows | Where-Object { $_.PageVerify -ne 'CHECKSUM' } | Measure-Object).Count
    if ($noChecksum -gt 0) {
        $recommendations.Add(@{
            Severity    = 'Medium'
            Category    = 'Configuration'
            Title       = "PAGE_VERIFY Not Set to CHECKSUM on $noChecksum Database(s)"
            Description = "CHECKSUM page verification detects torn pages and I/O errors. Enable it with: ALTER DATABASE [dbname] SET PAGE_VERIFY CHECKSUM. This has minimal performance impact and greatly improves data integrity detection."
        })
    }
}

# Statistics
if ($statsInfo -and $statsInfo.Rows.Count -gt 0) {
    $staleStats = ($statsInfo.Rows | Where-Object { [long]$_.ModificationsSinceUpdate -gt 50000 } | Measure-Object).Count
    if ($staleStats -gt 0) {
        $recommendations.Add(@{
            Severity    = 'Medium'
            Category    = 'Statistics'
            Title       = "Stale Statistics on $staleStats Object(s)"
            Description = "Statistics with high modification counters produce poor cardinality estimates leading to bad query plans. Run UPDATE STATISTICS for the affected tables, enable AUTO_UPDATE_STATISTICS_ASYNC, and consider setting a smaller auto-update threshold for large tables via trace flag 2371 (SQL 2014-) or automatic recompile thresholds (SQL 2016+)."
        })
    }
}

if ($recommendations.Count -eq 0) {
    $recommendations.Add(@{
        Severity    = 'Good'
        Category    = 'General'
        Title       = "No Critical Issues Found in Analysis Window"
        Description = "The automated analysis did not detect critical issues in the selected time window. Continue monitoring key metrics: wait stats, query duration trends, index fragmentation, and blocking."
    })
}

#endregion

#region ── HTML Generation ────────────────────────────────────────────────────

Write-Log "Generating HTML report..."

$reportDate   = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
$windowStr    = "$($StartTime.ToString('yyyy-MM-dd HH:mm')) → $($EndTime.ToString('yyyy-MM-dd HH:mm'))"
$serverName   = if ($serverInfo -and $serverInfo.Rows.Count -gt 0) { $serverInfo.Rows[0].ServerName } else { $ServerInstance }
$sqlVersion   = if ($serverInfo -and $serverInfo.Rows.Count -gt 0) { "$($serverInfo.Rows[0].Edition) $($serverInfo.Rows[0].ProductVersion)" } else { 'N/A' }
$totalDBs     = $databases.Rows.Count
$criticalRecs = ($recommendations | Where-Object { $_.Severity -eq 'Critical' } | Measure-Object).Count
$highRecs     = ($recommendations | Where-Object { $_.Severity -eq 'High' } | Measure-Object).Count

# Build recommendations HTML
$recsHtml = [System.Text.StringBuilder]::new()
foreach ($r in $recommendations) {
    $badge = Get-SeverityBadge -Level $r.Severity
    $null = $recsHtml.Append(@"
<div class="rec-card rec-$($r.Severity.ToLower())">
    <div class="rec-header">
        $badge
        <span class="rec-category">$([System.Web.HttpUtility]::HtmlEncode($r.Category))</span>
        <span class="rec-title">$([System.Web.HttpUtility]::HtmlEncode($r.Title))</span>
    </div>
    <div class="rec-body">$([System.Web.HttpUtility]::HtmlEncode($r.Description))</div>
</div>
"@)
}

$html = @"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>SQL Server Performance Report — $serverName</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
/* ── Reset & Base ── */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
    --bg:#f0f4f8;--surface:#ffffff;--surface2:#f8fafc;--surface3:#edf2f7;
    --border:#e2e8f0;--border2:#cbd5e0;
    --text:#1a202c;--text-muted:#4a5568;--text-dim:#a0aec0;
    --accent:#4f46e5;--accent2:#0284c7;--accent3:#db2777;
    --green:#059669;--yellow:#d97706;--orange:#ea580c;--red:#dc2626;
    --chart1:#4f46e5;--chart2:#0284c7;--chart3:#db2777;--chart4:#059669;
    --chart5:#d97706;--chart6:#ea580c;--chart7:#7c3aed;--chart8:#0891b2;
    --radius:12px;--radius-sm:8px;
    --shadow:0 1px 3px rgba(0,0,0,.08),0 1px 2px rgba(0,0,0,.05);
    --shadow-md:0 4px 12px rgba(0,0,0,.1);
    --font-mono:'Consolas','Fira Code','Courier New',monospace;
}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;font-size:14px;line-height:1.6}
a{color:var(--accent2);text-decoration:none}

/* ── Layout ── */
.wrapper{max-width:1600px;margin:0 auto;padding:24px 28px}

/* ── Header ── */
.report-header{
    background:linear-gradient(135deg,#4f46e5 0%,#0284c7 100%);
    border:none;border-radius:var(--radius);
    padding:32px 40px;margin-bottom:28px;
    box-shadow:var(--shadow-md);position:relative;overflow:hidden;
}
.report-header::before{
    content:'';position:absolute;top:-50%;right:-10%;width:400px;height:400px;
    background:radial-gradient(circle,rgba(255,255,255,.15) 0%,transparent 70%);
    pointer-events:none;
}
.report-header h1{font-size:28px;font-weight:700;color:#ffffff;margin-bottom:6px}
.report-header h1 span{color:#bfdbfe}
.report-header .meta{color:rgba(255,255,255,.8);font-size:13px;display:flex;flex-wrap:wrap;gap:20px;margin-top:12px}
.report-header .meta span{display:flex;align-items:center;gap:6px}
.report-header .meta span::before{content:'';width:8px;height:8px;border-radius:50%;background:rgba(255,255,255,.6)}

/* ── KPI Grid ── */
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:16px;margin-bottom:28px}
.kpi-card{
    background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
    padding:20px;position:relative;overflow:hidden;transition:transform .2s,border-color .2s,box-shadow .2s;
    box-shadow:var(--shadow);
}
.kpi-card:hover{transform:translateY(-2px);border-color:var(--accent);box-shadow:var(--shadow-md)}
.kpi-card::after{
    content:'';position:absolute;bottom:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,var(--accent),var(--accent2));
}
.kpi-card.warn::after{background:linear-gradient(90deg,var(--orange),var(--red))}
.kpi-card.ok::after{background:linear-gradient(90deg,var(--green),var(--accent2))}
.kpi-label{font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:var(--text-muted);font-weight:600;margin-bottom:8px}
.kpi-value{font-size:26px;font-weight:700;color:var(--accent)}
.kpi-sub{font-size:11px;color:var(--text-muted);margin-top:4px}

/* ── Sections ── */
.section{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:24px;overflow:hidden;box-shadow:var(--shadow)}
.section-header{
    padding:18px 24px;border-bottom:1px solid var(--border);
    background:linear-gradient(90deg,var(--surface2),var(--surface));
    display:flex;align-items:center;gap:12px;cursor:pointer;user-select:none;border-bottom:1px solid var(--border);
}
.section-header:hover{background:var(--surface3);border-bottom-color:var(--accent)}
.section-icon{font-size:18px}
.section-title{font-size:15px;font-weight:700;color:var(--text)}
.section-subtitle{font-size:12px;color:var(--text-muted);margin-left:auto;background:var(--surface3);padding:3px 10px;border-radius:20px;border:1px solid var(--border)}
.collapse-btn{margin-left:auto;color:var(--text-muted);font-size:18px;transition:transform .3s}
.section-body{padding:20px}
.section-body.collapsed{display:none}

/* ── Charts ── */
.chart-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(480px,1fr));gap:20px;margin-bottom:20px}
.chart-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:20px;box-shadow:var(--shadow)}
.chart-title{font-size:12px;font-weight:700;color:var(--text-muted);margin-bottom:16px;text-transform:uppercase;letter-spacing:.7px;border-bottom:2px solid var(--border);padding-bottom:8px}
.chart-wrap{position:relative;height:280px}

/* ── Tables ── */
.table-wrapper{overflow-x:auto;border-radius:var(--radius-sm)}
table.data-table{width:100%;border-collapse:collapse;font-size:13px}
table.data-table th{
    background:var(--surface2);color:var(--text-muted);
    padding:10px 14px;text-align:left;font-weight:700;
    font-size:11px;text-transform:uppercase;letter-spacing:.6px;
    border-bottom:2px solid var(--border2);white-space:nowrap;
}
table.data-table td{
    padding:9px 14px;border-bottom:1px solid var(--border);
    color:var(--text);vertical-align:top;
}
table.data-table tr:last-child td{border-bottom:none}
table.data-table tr:hover td{background:rgba(79,70,229,.05)}
table.data-table td:first-child{font-family:var(--font-mono);font-size:12px}

/* SQL Text cells */
.sql-cell{font-family:var(--font-mono);font-size:11px;color:#0369a1;
    max-width:400px;white-space:pre-wrap;word-break:break-all;background:#f0f9ff;
    padding:6px 8px;border-radius:4px;border-left:3px solid #0284c7}

/* ── Recommendations ── */
.rec-card{border-radius:var(--radius-sm);margin-bottom:14px;overflow:hidden;border:1px solid var(--border)}
.rec-header{padding:12px 18px;display:flex;align-items:center;gap:10px;background:var(--surface3)}
.rec-category{font-size:11px;background:var(--surface);border:1px solid var(--border2);padding:2px 8px;border-radius:20px;color:var(--text-muted);font-weight:600}
.rec-title{font-weight:600;color:var(--text);font-size:14px}
.rec-body{padding:14px 18px;font-size:13px;color:var(--text-muted);line-height:1.7}
.rec-critical .rec-header{border-left:4px solid #dc2626;background:#fef2f2}
.rec-high .rec-header{border-left:4px solid #ea580c;background:#fff7ed}
.rec-medium .rec-header{border-left:4px solid #d97706;background:#fffbeb}
.rec-low .rec-header{border-left:4px solid #059669;background:#f0fdf4}
.rec-good .rec-header{border-left:4px solid #059669;background:#f0fdf4}

/* ── Badges ── */
.badge{display:inline-block;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.badge-critical{background:#fee2e2;color:#b91c1c;border:1px solid #fca5a5}
.badge-high{background:#ffedd5;color:#c2410c;border:1px solid #fdba74}
.badge-medium{background:#fef9c3;color:#a16207;border:1px solid #fde047}
.badge-low{background:#d1fae5;color:#065f46;border:1px solid #6ee7b7}
.badge-good{background:#d1fae5;color:#065f46;border:1px solid #6ee7b7}

/* ── Navigation ── */
.nav{
    position:sticky;top:0;z-index:100;background:rgba(255,255,255,.97);
    backdrop-filter:blur(10px);border-bottom:1px solid var(--border);
    box-shadow:0 1px 4px rgba(0,0,0,.07);
    padding:0 24px;display:flex;gap:2px;overflow-x:auto;margin-bottom:28px;
}
.nav a{
    padding:14px 16px;font-size:13px;font-weight:500;color:var(--text-muted);
    white-space:nowrap;border-bottom:2px solid transparent;transition:all .2s;display:block;
}
.nav a:hover,.nav a.active{color:var(--accent);border-bottom-color:var(--accent)}

/* ── Misc ── */
.no-data{color:var(--text-dim);font-style:italic;padding:24px;text-align:center;background:var(--surface2);border-radius:var(--radius-sm)}
.tag{display:inline-block;background:var(--surface3);border:1px solid var(--border);
    border-radius:4px;padding:2px 8px;font-size:11px;font-family:var(--font-mono);color:var(--text-muted)}
.progress-bar{height:6px;background:var(--surface3);border-radius:3px;overflow:hidden;margin-top:4px}
.progress-fill{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));border-radius:3px;transition:width .4s}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:20px}
@media(max-width:900px){.two-col{grid-template-columns:1fr}.chart-grid{grid-template-columns:1fr}}
.footer{text-align:center;color:var(--text-dim);font-size:12px;padding:24px;margin-top:8px;border-top:1px solid var(--border);background:var(--surface)}
.scroll-top{
    position:fixed;bottom:24px;right:24px;width:42px;height:42px;
    background:var(--accent);border:none;border-radius:50%;color:#fff;
    font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;
    box-shadow:0 4px 20px rgba(79,70,229,.35);transition:transform .2s;z-index:200;
}
.scroll-top:hover{transform:scale(1.1)}
</style>
</head>
<body>
<button class="scroll-top" onclick="window.scrollTo({top:0,behavior:'smooth'})">↑</button>

<nav class="nav" id="mainNav">
    <a href="#summary">Summary</a>
    <a href="#recommendations">⚠ Recommendations</a>
    <a href="#databases">Databases</a>
    <a href="#waits">Wait Stats</a>
    <a href="#cpu-queries">Top CPU Queries</a>
    <a href="#io-queries">Top I/O Queries</a>
    <a href="#exec-queries">Most Executed</a>
    <a href="#missing-idx">Missing Indexes</a>
    <a href="#unused-idx">Unused Indexes</a>
    <a href="#fragmentation">Fragmentation</a>
    <a href="#blocking">Blocking</a>
    <a href="#memory">Memory</a>
    <a href="#io-stats">File I/O</a>
    <a href="#tempdb">TempDB</a>
    <a href="#statistics">Statistics</a>
    <a href="#scheduler">Schedulers</a>
</nav>

<div class="wrapper">

<!-- ── HEADER ── -->
<div class="report-header">
    <h1>SQL Server Performance Report — <span>$serverName</span></h1>
    <div style="color:var(--text-muted);margin-top:6px;font-size:15px">$sqlVersion</div>
    <div class="meta">
        <span>Generated: $reportDate</span>
        <span>Analysis Window: $windowStr</span>
        <span>Databases Analyzed: $totalDBs</span>
        <span>Critical Findings: $criticalRecs</span>
        <span>High Findings: $highRecs</span>
    </div>
</div>

<!-- ── KPI CARDS ── -->
<div id="summary" class="kpi-grid">
"@

# Dynamic KPI cards
# KPI values — safe because Invoke-SqlQuery always returns a DataTable
$physMem    = if ($serverInfo.Rows.Count -gt 0)  { "$($serverInfo.Rows[0].PhysicalMemoryMB) MB" } else { "N/A" }
$logCPU     = if ($serverInfo.Rows.Count -gt 0)  { $serverInfo.Rows[0].LogicalCPUs }              else { "N/A" }
$sqlUptime  = if ($serverInfo.Rows.Count -gt 0) {
    try {
        $start = [datetime]$serverInfo.Rows[0].SQLStartTime
        $span  = (Get-Date) - $start
        "$([int]$span.TotalDays)d $($span.Hours)h $($span.Minutes)m"
    } catch { "N/A" }
} else { "N/A" }
$sqlMemUsed      = if ($memoryInfo.Rows.Count -gt 0)  { "$($memoryInfo.Rows[0].SQL_Physical_MB) MB" }    else { "N/A" }
$bufferMB        = if ($bufferPool.Rows.Count -gt 0)  { "$($bufferPool.Rows[0].TotalBufferMB) MB" }      else { "N/A" }
$missingIdxCount = $missingIdx.Rows.Count
$blockCount      = ($blocking.Rows | Where-Object { $_.BlockedBy -ne 0 } | Measure-Object).Count
$fragCount       = $fragmentation.Rows.Count
$waitTopStr      = if ($waitStats.Rows.Count -gt 0)   { $waitStats.Rows[0].WaitType }                    else { "N/A" }

$html += @"
    <div class="kpi-card ok">
        <div class="kpi-label">Physical Memory</div>
        <div class="kpi-value">$physMem</div>
        <div class="kpi-sub">Total installed RAM</div>
    </div>
    <div class="kpi-card ok">
        <div class="kpi-label">Logical CPUs</div>
        <div class="kpi-value">$logCPU</div>
        <div class="kpi-sub">Available to SQL Server</div>
    </div>
    <div class="kpi-card ok">
        <div class="kpi-label">SQL Memory Used</div>
        <div class="kpi-value">$sqlMemUsed</div>
        <div class="kpi-sub">Process physical memory</div>
    </div>
    <div class="kpi-card ok">
        <div class="kpi-label">Buffer Pool</div>
        <div class="kpi-value">$bufferMB</div>
        <div class="kpi-sub">Data pages cached</div>
    </div>
    <div class="kpi-card ok">
        <div class="kpi-label">SQL Uptime</div>
        <div class="kpi-value">$sqlUptime</div>
        <div class="kpi-sub">Since last restart</div>
    </div>
    <div class="kpi-card ok">
        <div class="kpi-label">Databases</div>
        <div class="kpi-value">$totalDBs</div>
        <div class="kpi-sub">Online user databases</div>
    </div>
    <div class="kpi-card $(if($blockCount -gt 0){'warn'}else{'ok'})">
        <div class="kpi-label">Blocked Sessions</div>
        <div class="kpi-value">$blockCount</div>
        <div class="kpi-sub">Currently blocked</div>
    </div>
    <div class="kpi-card $(if($missingIdxCount -gt 5){'warn'}else{'ok'})">
        <div class="kpi-label">Missing Indexes</div>
        <div class="kpi-value">$missingIdxCount</div>
        <div class="kpi-sub">Recommended by SQL Server</div>
    </div>
    <div class="kpi-card $(if($fragCount -gt 10){'warn'}else{'ok'})">
        <div class="kpi-label">Fragmented Indexes</div>
        <div class="kpi-value">$fragCount</div>
        <div class="kpi-sub">>10% fragmentation</div>
    </div>
    <div class="kpi-card $(if($criticalRecs -gt 0){'warn'}else{'ok'})">
        <div class="kpi-label">Top Wait Type</div>
        <div class="kpi-value" style="font-size:14px;padding-top:4px">$waitTopStr</div>
        <div class="kpi-sub">Dominant wait category</div>
    </div>
</div>

<!-- ── CHARTS ROW 1 ── -->
<div class="chart-grid">
    <div class="chart-card">
        <div class="chart-title">🕐 Top 10 Wait Types (Total Wait Seconds)</div>
        <div class="chart-wrap"><canvas id="chartWaits"></canvas></div>
    </div>
    <div class="chart-card">
        <div class="chart-title">💿 Database Sizes (MB)</div>
        <div class="chart-wrap"><canvas id="chartDBSizes"></canvas></div>
    </div>
</div>
<div class="chart-grid">
    <div class="chart-card">
        <div class="chart-title">⚡ Top 10 CPU Queries (Total CPU ms)</div>
        <div class="chart-wrap"><canvas id="chartCPU"></canvas></div>
    </div>
    <div class="chart-card">
        <div class="chart-title">🗄 Buffer Pool by Database (MB)</div>
        <div class="chart-wrap"><canvas id="chartBuffer"></canvas></div>
    </div>
</div>
<div class="chart-grid">
    <div class="chart-card">
        <div class="chart-title">💾 File I/O Latency (ms) — Reads vs Writes</div>
        <div class="chart-wrap"><canvas id="chartIO"></canvas></div>
    </div>
    <div class="chart-card">
        <div class="chart-title">🔧 Index Fragmentation % (Top 10)</div>
        <div class="chart-wrap"><canvas id="chartFrag"></canvas></div>
    </div>
</div>

<!-- ── RECOMMENDATIONS ── -->
<div id="recommendations" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">⚠️</span>
        <span class="section-title">Recommendations & Findings</span>
        <span class="section-subtitle">$($recommendations.Count) items · $(if($criticalRecs -gt 0){"$criticalRecs Critical"}else{"No Critical Issues"})</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $($recsHtml.ToString())
    </div>
</div>

<!-- ── DATABASES ── -->
<div id="databases" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🗃</span>
        <span class="section-title">Database Inventory</span>
        <span class="section-subtitle">$totalDBs databases</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $databases)
    </div>
</div>

<!-- ── WAIT STATS ── -->
<div id="waits" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">⏱</span>
        <span class="section-title">Wait Statistics</span>
        <span class="section-subtitle">Cumulative since last restart</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $waitStats)
    </div>
</div>

<!-- ── TOP CPU ── -->
<div id="cpu-queries" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🔥</span>
        <span class="section-title">Top CPU Queries</span>
        <span class="section-subtitle">Top $TopN by total worker time</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $topCPU)
    </div>
</div>

<!-- ── TOP IO ── -->
<div id="io-queries" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">📖</span>
        <span class="section-title">Top I/O Queries</span>
        <span class="section-subtitle">Top $TopN by total logical reads + writes</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $topIO)
    </div>
</div>

<!-- ── MOST EXECUTED ── -->
<div id="exec-queries" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🔁</span>
        <span class="section-title">Most Frequently Executed Queries</span>
        <span class="section-subtitle">Top $TopN by execution count</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $topExec)
    </div>
</div>

<!-- ── MISSING INDEXES ── -->
<div id="missing-idx" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🔍</span>
        <span class="section-title">Missing Index Recommendations</span>
        <span class="section-subtitle">$missingIdxCount candidates with CREATE scripts</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $missingIdx)
    </div>
</div>

<!-- ── UNUSED INDEXES ── -->
<div id="unused-idx" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🗑</span>
        <span class="section-title">Unused Indexes</span>
        <span class="section-subtitle">Indexes with zero reads but active writes (drop candidates)</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $unusedIdx)
    </div>
</div>

<!-- ── FRAGMENTATION ── -->
<div id="fragmentation" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🧩</span>
        <span class="section-title">Index Fragmentation</span>
        <span class="section-subtitle">$fragCount indexes with >10% fragmentation</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $fragmentation)
    </div>
</div>

<!-- ── BLOCKING ── -->
<div id="blocking" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🚫</span>
        <span class="section-title">Active Sessions & Blocking</span>
        <span class="section-subtitle">Snapshot at report generation time</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $blocking -EmptyMessage 'No active sessions with blocking detected.')
    </div>
</div>

<!-- ── MEMORY ── -->
<div id="memory" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">💾</span>
        <span class="section-title">Memory Overview</span>
        <span class="section-subtitle">Process memory, buffer pool, and allocation</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        <div class="two-col">
            <div>
                <div class="chart-title" style="margin-bottom:12px">Process Memory</div>
                $(ConvertTo-HtmlTable -Table $memoryInfo)
            </div>
            <div>
                <div class="chart-title" style="margin-bottom:12px">Buffer Pool Summary</div>
                $(ConvertTo-HtmlTable -Table $bufferPool)
            </div>
        </div>
        <br/>
        <div class="chart-title" style="margin-bottom:12px">Buffer Pool by Database</div>
        $(ConvertTo-HtmlTable -Table $bufferByDB)
    </div>
</div>

<!-- ── FILE I/O ── -->
<div id="io-stats" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">💿</span>
        <span class="section-title">File I/O Statistics</span>
        <span class="section-subtitle">Read/write latency per database file</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $ioStats)
    </div>
</div>

<!-- ── TEMPDB ── -->
<div id="tempdb" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">🌡</span>
        <span class="section-title">TempDB Usage by Session</span>
        <span class="section-subtitle">Top sessions consuming TempDB space</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $tempDBUsage -EmptyMessage 'No sessions currently using significant TempDB space.')
    </div>
</div>

<!-- ── STATISTICS ── -->
<div id="statistics" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">📊</span>
        <span class="section-title">Stale Statistics</span>
        <span class="section-subtitle">Objects with high modification counters or outdated stats</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $statsInfo -EmptyMessage 'No stale statistics detected.')
    </div>
</div>

<!-- ── SCHEDULERS ── -->
<div id="scheduler" class="section">
    <div class="section-header" onclick="toggleSection(this)">
        <span class="section-icon">⚙️</span>
        <span class="section-title">CPU Scheduler Load</span>
        <span class="section-subtitle">OS schedulers and runnable task queue</span>
        <span class="collapse-btn">▼</span>
    </div>
    <div class="section-body">
        $(ConvertTo-HtmlTable -Table $schedulers)
    </div>
</div>

<div class="footer">
    SQL Server Performance Report · Generated by SQL-Performance-Report.ps1 · $reportDate<br/>
    Server: $serverName · Edition: $sqlVersion
</div>
</div><!-- /wrapper -->

<script>
// ── Chart.js Defaults ──
Chart.defaults.color = '#64748b';
Chart.defaults.borderColor = '#e2e8f0';
Chart.defaults.font.family = '-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif';
Chart.defaults.font.size = 12;

const COLORS = ['#6c63ff','#00d9ff','#ff6b9d','#00e676','#ffd740','#ff6d00','#a78bfa','#34d399','#f59e0b','#60a5fa'];

function makeBarChart(id, labels, data, label, color, horizontal) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{ label, data, backgroundColor: color + 'cc', borderColor: color, borderWidth: 1, borderRadius: 4 }]
        },
        options: {
            indexAxis: horizontal ? 'y' : 'x',
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ' ' + c.formattedValue } } },
            scales: {
                x: { grid: { color: '#f1f5f9' }, ticks: { maxRotation: horizontal ? 0 : 45, color:'#64748b' } },
                y: { grid: { color: '#f1f5f9' }, ticks: { color:'#64748b' } }
            }
        }
    });
}

function makeDoughnutChart(id, labels, data) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    new Chart(ctx, {
        type: 'doughnut',
        data: { labels, datasets: [{ data, backgroundColor: COLORS, borderColor: '#ffffff', borderWidth: 2, hoverOffset: 8 }] },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'right', labels: { boxWidth: 12, padding: 14, font: { size: 11 } } },
                tooltip: { callbacks: { label: c => ' ' + c.label + ': ' + c.formattedValue } }
            }
        }
    });
}

function makeGroupedBarChart(id, labels, datasets) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top', labels: { boxWidth: 12, padding: 14 } },
                       tooltip: { callbacks: { label: c => ' ' + c.dataset.label + ': ' + c.formattedValue + ' ms' } } },
            scales: {
                x: { grid: { color: '#f1f5f9' }, ticks: { maxRotation: 45, color:'#64748b' } },
                y: { grid: { color: '#f1f5f9' }, ticks: { color:'#64748b' }, title: { display: true, text: 'ms', color: '#64748b' } }
            }
        }
    });
}

// ── Wait Stats Chart ──
makeBarChart('chartWaits', $waitLabels, $waitValues, 'Wait Seconds', '#6c63ff', true);

// ── DB Sizes Chart ──
makeDoughnutChart('chartDBSizes', $dbSizeLabels, $dbSizeValues);

// ── CPU Queries ──
makeBarChart('chartCPU', $cpuQueryLabels, $cpuQueryValues, 'Total CPU (ms)', '#ff6b9d', true);

// ── Buffer Pool Chart ──
makeDoughnutChart('chartBuffer', $bufLabels, $bufValues);

// ── I/O Latency Chart ──
makeGroupedBarChart('chartIO', $ioLabels, [
    { label: 'Avg Read Latency (ms)',  data: $ioReadValues,  backgroundColor: '#6c63ffcc', borderColor: '#6c63ff', borderRadius: 4 },
    { label: 'Avg Write Latency (ms)', data: $ioWriteValues, backgroundColor: '#ff6b9dcc', borderColor: '#ff6b9d', borderRadius: 4 }
]);

// ── Fragmentation Chart ──
makeBarChart('chartFrag', $fragLabels, $fragValues, 'Fragmentation %', '#ffd740', true);

// ── Collapse sections ──
function toggleSection(header) {
    const body = header.nextElementSibling;
    const btn  = header.querySelector('.collapse-btn');
    if (body.classList.contains('collapsed')) {
        body.classList.remove('collapsed');
        btn.style.transform = 'rotate(0deg)';
    } else {
        body.classList.add('collapsed');
        btn.style.transform = 'rotate(-90deg)';
    }
}

// ── Sticky nav active ──
const sections = document.querySelectorAll('[id]');
const navLinks  = document.querySelectorAll('.nav a');
window.addEventListener('scroll', () => {
    let current = '';
    sections.forEach(s => { if (window.scrollY >= s.offsetTop - 90) current = s.id; });
    navLinks.forEach(a => {
        a.classList.toggle('active', a.getAttribute('href') === '#' + current);
    });
}, { passive: true });
</script>
</body>
</html>
"@

#endregion

#region ── Write Output ────────────────────────────────────────────────────────

$filename  = "SQLPerformanceReport_$($serverName -replace '[\\\/\:]','_')_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"
$outFile   = Join-Path $ReportPath $filename

Write-Log "Writing report → $outFile"
[System.IO.File]::WriteAllText($outFile, $html, [System.Text.Encoding]::UTF8)

$sizeKB = [math]::Round((Get-Item $outFile).Length / 1KB, 1)
Write-Log "Report written: $outFile ($sizeKB KB)" 'OK'
Write-Log "=== Done ===" 'OK'

# Optionally open in browser
if ($env:TERM_PROGRAM -ne 'vscode' -and -not $env:CI) {
    $open = Read-Host "Open report in browser? (Y/n)"
    if ($open -ne 'n' -and $open -ne 'N') {
        Start-Process $outFile
    }
}

return $outFile

#endregion
