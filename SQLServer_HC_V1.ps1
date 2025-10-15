<# 
  SqlHealthMonitor.ps1
  Windows PowerShell 5.1+ (or PowerShell 7) | .NET WPF
  Files created/used:
    - servers.csv (in same folder): Instance,Environment
    - .\config\settings.json : thresholds & UI layout persistence
#>

[CmdletBinding()]
param(
  [string]$ServerListPath = ".\servers.csv",
  [int]$MaxParallel = 12
)

#region --- Helpers & Config ---
Add-Type -AssemblyName PresentationFramework, PresentationCore, WindowsBase

$AppRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ConfigDir = Join-Path $AppRoot "config"
$SettingsPath = Join-Path $ConfigDir "settings.json"

if (!(Test-Path $ConfigDir)) { New-Item -ItemType Directory -Path $ConfigDir | Out-Null }

# Default settings
$DefaultSettings = [ordered]@{
  RefreshMinutes        = 5
  BackupWarnDays        = 2
  BackupCritDays        = 4
  DiskWarnPct           = 85
  DiskCritPct           = 92
  EnableParallel        = $true
  MaxParallel           = $MaxParallel
  Window = @{
    Width  = 1300
    Height = 700
  }
  ColumnOrder = @()   # array of column header names, persisted after user drag/drops
}

function Load-Settings {
  if (Test-Path $SettingsPath) {
    try {
      return Get-Content $SettingsPath -Raw | ConvertFrom-Json -Depth 6
    } catch {
      return ($DefaultSettings | ConvertTo-Json | ConvertFrom-Json)
    }
  }
  else {
    $DefaultSettings | ConvertTo-Json -Depth 6 | Out-File -Encoding UTF8 $SettingsPath
    return ($DefaultSettings | ConvertTo-Json | ConvertFrom-Json)
  }
}

function Save-Settings ($settings) {
  ($settings | ConvertTo-Json -Depth 6) | Out-File -Encoding UTF8 $SettingsPath
}

$Settings = Load-Settings

# Load server list
if (!(Test-Path $ServerListPath)) {
  @"Instance,Environment
(LOCALHOST)\SQLEXPRESS,DEV
MyProdSql01,PROD
MyUatSql02,UAT
"@ | Out-File -Encoding UTF8 $ServerListPath
}
$Servers = Import-Csv $ServerListPath

# SQL helper – run T-SQL and return DataTable
function Invoke-SqlQuery {
  param(
    [Parameter(Mandatory)][string]$Instance,
    [Parameter(Mandatory)][string]$Query,
    [int]$TimeoutSec = 30
  )
  $cn = New-Object System.Data.SqlClient.SqlConnection
  $cn.ConnectionString = "Server=$Instance;Integrated Security=True;Database=master;TrustServerCertificate=True"
  $cmd = $cn.CreateCommand()
  $cmd.CommandText = $Query
  $cmd.CommandTimeout = $TimeoutSec
  $dt = New-Object System.Data.DataTable
  try {
    $cn.Open()
    $rdr = $cmd.ExecuteReader()
    $dt.Load($rdr)
    $rdr.Close()
    $cn.Close()
    return ,$dt
  } catch {
    if ($cn.State -eq 'Open') { $cn.Close() }
    throw
  }
}

# One-shot collector per instance
function Test-Instance {
  param(
    [Parameter(Mandatory)][string]$Instance,
    [string]$Environment,
    [hashtable]$Settings
  )

  $result = [ordered]@{
    'S.No'                       = 0
    'SQL Server Instance'        = $Instance
    'Environment'                = $Environment
    'Version'                    = ''
    'CU'                         = ''
    'Instance Status'            = 'Down'
    'Agent Status'               = 'Unknown'
    'totaldatabases/online databases' = ''
    'Oldest date of Last full backup' = ''
    'Disk size with %'           = ''
    'Last checked'               = (Get-Date)
    'Check Status'               = 'CRIT'
    'Error'                      = ''
  }

  $qCore = @"
SELECT
  CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(50)) AS ProductVersion,
  CAST(SERVERPROPERTY('ProductLevel')   AS nvarchar(50)) AS ProductLevel,     -- RTM/SP/CU-level text
  CAST(SERVERPROPERTY('ProductUpdateLevel') AS nvarchar(50)) AS ProductUpdateLevel, -- 'CU14' for 2017+, else NULL
  CAST(SERVERPROPERTY('Edition')        AS nvarchar(128)) AS Edition
"@

  $qDbCounts = @"
SELECT
  COUNT(*) AS totaldb,
  SUM(CASE WHEN state_desc='ONLINE' THEN 1 ELSE 0 END) AS onlinedb
FROM sys.databases
"@

  # Agent status from host via DMV (needs VIEW SERVER STATE; else NULL)
  $qAgent = @"
SELECT TOP 1
  status_desc
FROM sys.dm_server_services
WHERE servicename LIKE 'SQL Server Agent%'
"@

  # Oldest (stale) full backup date across databases
  $qBackup = @"
;WITH lastfull AS (
  SELECT
    bs.database_name,
    MAX(bs.backup_finish_date) AS last_full_backup_finish_date
  FROM msdb.dbo.backupset bs
  WHERE bs.type = 'D'
  GROUP BY bs.database_name
)
SELECT MIN(last_full_backup_finish_date) AS oldest_full_backup
FROM lastfull
"@

  # Worst disk volume usage (% used) using dm_os_volume_stats
  $qDisk = @"
;WITH vols AS (
  SELECT DISTINCT
    vs.volume_mount_point,
    vs.total_bytes,
    vs.available_bytes,
    CAST(100.0 - (100.0 * vs.available_bytes / NULLIF(vs.total_bytes,0)) AS decimal(5,2)) AS used_pct
  FROM sys.master_files mf
  CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
)
SELECT TOP 1
  volume_mount_point,
  total_bytes,
  available_bytes,
  used_pct
FROM vols
ORDER BY used_pct DESC
"@

  try {
    # Quick connectivity check & gather
    $dtCore   = Invoke-SqlQuery -Instance $Instance -Query $qCore
    $result['Instance Status'] = 'Up'

    $ver = $dtCore.Rows[0].ProductVersion
    $cul = $dtCore.Rows[0].ProductUpdateLevel
    $result['Version'] = $ver
    $result['CU']      = [string]::IsNullOrEmpty($cul) ? '' : $cul

    $dtDb    = Invoke-SqlQuery -Instance $Instance -Query $qDbCounts
    $totaldb = [int]$dtDb.Rows[0].totaldb
    $onlinedb= [int]$dtDb.Rows[0].onlinedb
    $result['totaldatabases/online databases'] = "{0}/{1}" -f $totaldb, $onlinedb

    # Agent
    try {
      $dtAgent = Invoke-SqlQuery -Instance $Instance -Query $qAgent -TimeoutSec 20
      if ($dtAgent.Rows.Count -gt 0 -and $dtAgent.Rows[0].status_desc) {
        $result['Agent Status'] = $dtAgent.Rows[0].status_desc
      } else {
        $result['Agent Status'] = 'Unknown'
      }
    } catch {
      $result['Agent Status'] = 'Unknown'
    }

    # Backups
    $dtBkp = Invoke-SqlQuery -Instance $Instance -Query $qBackup
    $oldest = $null
    if ($dtBkp.Rows.Count -gt 0) { $oldest = $dtBkp.Rows[0].oldest_full_backup }
    if ($oldest) {
      $result['Oldest date of Last full backup'] = [DateTime]$oldest
    } else {
      $result['Oldest date of Last full backup'] = $null
    }

    # Disk worst usage
    try {
      $dtDisk = Invoke-SqlQuery -Instance $Instance -Query $qDisk
      if ($dtDisk.Rows.Count -gt 0) {
        $mp   = $dtDisk.Rows[0].volume_mount_point
        $totB = [double]$dtDisk.Rows[0].total_bytes
        $avB  = [double]$dtDisk.Rows[0].available_bytes
        $used = [decimal]$dtDisk.Rows[0].used_pct
        $totGB = [math]::Round($totB/1GB,2)
        $usedGB= [math]::Round(($totB-$avB)/1GB,2)
        $result['Disk size with %'] = "{0} {1}/{2} GB ({3}%)" -f $mp, $usedGB, $totGB, $used
        $worstUsedPct = [double]$used
      } else {
        $result['Disk size with %'] = ''
        $worstUsedPct = $null
      }
    } catch {
      $result['Disk size with %'] = ''
      $worstUsedPct = $null
    }

    # Determine Check Status
    $crit = $false; $warn = $false; $notes = @()

    if ($result['Instance Status'] -ne 'Up') { $crit = $true; $notes += 'Instance Down' }

    if ($result['Agent Status'] -eq 'Stopped') { $warn = $true; $notes += 'Agent Stopped' }

    if ($onlinedb -lt $totaldb) { $warn = $true; $notes += 'Some DBs not ONLINE' }

    $now = Get-Date
    $backupDaysOld = $null
    if ($result['Oldest date of Last full backup']) {
      $backupDaysOld = [int]($now - [DateTime]$result['Oldest date of Last full backup']).TotalDays
      if ($backupDaysOld -ge $Settings.BackupCritDays) { $crit = $true; $notes += "Oldest full backup ${backupDaysOld}d" }
      elseif ($backupDaysOld -ge $Settings.BackupWarnDays) { $warn = $true; $notes += "Oldest full backup ${backupDaysOld}d" }
    } else {
      $warn = $true; $notes += 'No full backups found'
    }

    if ($worstUsedPct -ne $null) {
      if ($worstUsedPct -ge $Settings.DiskCritPct) { $crit = $true; $notes += "Disk ${worstUsedPct}%" }
      elseif ($worstUsedPct -ge $Settings.DiskWarnPct) { $warn = $true; $notes += "Disk ${worstUsedPct}%" }
    }

    $result['Check Status'] = if ($crit) { 'CRIT' } elseif ($warn) { 'WARN' } else { 'OK' }
    $result['Error'] = ($notes -join '; ')
    return [pscustomobject]$result

  } catch {
    $result['Error'] = $_.Exception.Message
    $result['Check Status'] = 'CRIT'
    return [pscustomobject]$result
  }
}

# Runspace pool for parallelism
function Get-Results {
  param([System.Collections.IEnumerable]$Servers, [hashtable]$Settings)

  $list = New-Object System.Collections.Concurrent.ConcurrentBag[object]

  $scriptBlock = {
    param($row, $settings)
    Test-Instance -Instance $row.Instance -Environment $row.Environment -Settings $settings
  }

  if ($Settings.EnableParallel) {
    $jobs = @()
    $throttle = [math]::Max(1, [int]$Settings.MaxParallel)
    $sessionState = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
    $pool = [runspacefactory]::CreateRunspacePool(1, $throttle, $sessionState, $Host)
    $pool.Open()

    foreach ($r in $Servers) {
      $ps = [powershell]::Create().AddScript($scriptBlock).AddArgument($r).AddArgument($Settings)
      $ps.RunspacePool = $pool
      $jobs += [pscustomobject]@{ PS=$ps; Handle=$ps.BeginInvoke() }
    }
    $i = 0
    foreach ($j in $jobs) {
      $out = $j.PS.EndInvoke($j.Handle)
      foreach ($o in $out) { $list.Add($o) }
      $j.PS.Dispose()
      $i++
    }
    $pool.Close()
    $pool.Dispose()
  } else {
    foreach ($r in $Servers) {
      $o = Test-Instance -Instance $r.Instance -Environment $r.Environment -Settings $Settings
      $list.Add($o)
    }
  }

  # Add serial numbers
  $arr = $list.ToArray() | Sort-Object 'SQL Server Instance'
  for ($i=0; $i -lt $arr.Count; $i++) { $arr[$i].'S.No' = $i+1 }
  return ,$arr
}
#endregion

#region --- XAML UI ---
$Xaml = @"
<Window xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
        xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
        Title="SQL Server Health Monitor" Height="$($Settings.Window.Height)" Width="$($Settings.Window.Width)" WindowStartupLocation="CenterScreen">
  <DockPanel LastChildFill="True">
    <Menu DockPanel.Dock="Top">
      <MenuItem Header="_File">
        <MenuItem Header="Export to CSV" Name="ExportCsvMenu"/>
        <Separator/>
        <MenuItem Header="Exit" Name="ExitMenu"/>
      </MenuItem>
      <MenuItem Header="_View">
        <MenuItem Header="Refresh Now" Name="RefreshMenu"/>
      </MenuItem>
      <MenuItem Header="_Settings" Name="SettingsMenu"/>
      <MenuItem Header="_Help">
        <MenuItem Header="Open servers.csv" Name="OpenServers"/>
      </MenuItem>
    </Menu>
    <ToolBar DockPanel.Dock="Top">
      <Button Name="RefreshBtn" Padding="10,3">Refresh</Button>
      <Button Name="ExportBtn" Padding="10,3">Export CSV</Button>
      <Separator/>
      <TextBlock Text="Auto refresh (min):" Margin="10,0,5,0"/>
      <TextBox Name="AutoRefBox" Width="40" Text="$($Settings.RefreshMinutes)"/>
      <Button Name="ApplyAuto" Padding="10,3">Apply</Button>
      <Separator/>
      <Button Name="SettingsBtn" Padding="10,3">Thresholds</Button>
      <TextBlock Name="StatusText" Margin="20,0,0,0" VerticalAlignment="Center"/>
    </ToolBar>
    <DataGrid Name="Grid" AutoGenerateColumns="True" CanUserReorderColumns="True" CanUserResizeColumns="True" CanUserSortColumns="True"
              IsReadOnly="True" Margin="5" AlternationCount="2">
      <DataGrid.RowStyle>
        <Style TargetType="DataGridRow">
          <Setter Property="Background" Value="{DynamicResource {x:Static SystemColors.WindowBrushKey}}"/>
          <Style.Triggers>
            <DataTrigger Binding="{Binding Path=Check Status}" Value="CRIT">
              <Setter Property="Background" Value="#FFFDE7E9"/>
            </DataTrigger>
            <DataTrigger Binding="{Binding Path=Check Status}" Value="WARN">
              <Setter Property="Background" Value="#FFFFF8E1"/>
            </DataTrigger>
            <DataTrigger Binding="{Binding Path=Check Status}" Value="OK">
              <Setter Property="Background" Value="#FFE8F5E9"/>
            </DataTrigger>
          </Style.Triggers>
        </Style>
      </DataGrid.RowStyle>
    </DataGrid>
    <StatusBar DockPanel.Dock="Bottom">
      <StatusBarItem>
        <TextBlock Name="Footer" />
      </StatusBarItem>
    </StatusBar>
  </DockPanel>
</Window>
"@

$reader = New-Object System.Xml.XmlNodeReader ([xml]$Xaml)
$Window = [Windows.Markup.XamlReader]::Load($reader)
$Grid   = $Window.FindName('Grid')
$StatusText = $Window.FindName('StatusText')
$Footer = $Window.FindName('Footer')

$RefreshBtn  = $Window.FindName('RefreshBtn')
$ExportBtn   = $Window.FindName('ExportBtn')
$ApplyAuto   = $Window.FindName('ApplyAuto')
$AutoRefBox  = $Window.FindName('AutoRefBox')
$SettingsBtn = $Window.FindName('SettingsBtn')

$ExportCsvMenu = $Window.FindName('ExportCsvMenu')
$ExitMenu      = $Window.FindName('ExitMenu')
$RefreshMenu   = $Window.FindName('RefreshMenu')
$OpenServers   = $Window.FindName('OpenServers')
$SettingsMenu  = $Window.FindName('SettingsMenu')

# Apply persisted column order (after first data bind we’ll finalize)
$PersistedColumnOrder = @($Settings.ColumnOrder)
#endregion

#region --- Bind & Events ---
$global:CurrentData = @()

function Bind-Data($data) {
  $Grid.ItemsSource = $null
  $Grid.ItemsSource = $data

  # Re-apply user column order if saved
  if ($PersistedColumnOrder.Count -gt 0) {
    foreach ($colName in $PersistedColumnOrder) {
      $col = $Grid.Columns | Where-Object { $_.Header -eq $colName }
      if ($null -ne $col) {
        $idx = [array]::IndexOf($PersistedColumnOrder, $colName)
        if ($idx -ge 0) { $col.DisplayIndex = $idx }
      }
    }
  }
}

function Refresh-Now {
  $StatusText.Text = "Checking..."
  $Window.Cursor = 'Wait'
  try {
    $data = Get-Results -Servers $Servers -Settings $Settings
    $global:CurrentData = $data
    Bind-Data $data
    $Footer.Text = "Instances: $($data.Count) | Refreshed: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    $StatusText.Text = "Done"
  } catch {
    $StatusText.Text = "Error: $($_.Exception.Message)"
  } finally {
    $Window.Cursor = 'Arrow'
  }
}

# Export
function Export-CSV {
  if ($global:CurrentData.Count -eq 0) { return }
  $path = Join-Path $AppRoot ("SqlHealth_{0:yyyyMMdd_HHmmss}.csv" -f (Get-Date))
  $global:CurrentData | Export-Csv -NoTypeInformation -Encoding UTF8 $path
  [System.Windows.MessageBox]::Show("Exported to `n$path","Export", 'OK','Information') | Out-Null
}

# Threshold dialog (simple Prompt)
function Edit-Thresholds {
  $x = @"
BackupWarnDays (current: $($Settings.BackupWarnDays))
BackupCritDays (current: $($Settings.BackupCritDays))
DiskWarnPct    (current: $($Settings.DiskWarnPct))
DiskCritPct    (current: $($Settings.DiskCritPct))
MaxParallel    (current: $($Settings.MaxParallel))
(Enter values as name=value per line; blank line to keep)
"@
  $inp = [Microsoft.VisualBasic.Interaction]::InputBox($x, "Thresholds / Parallelism", "")
  if ($inp) {
    foreach ($line in $inp -split "`r?`n") {
      if ($line -match "=") {
        $k,$v = $line -split "=",2
        $k = $k.Trim(); $v=$v.Trim()
        if ($k -and $v) {
          switch ($k) {
            'BackupWarnDays' { $Settings.BackupWarnDays = [int]$v }
            'BackupCritDays' { $Settings.BackupCritDays = [int]$v }
            'DiskWarnPct'    { $Settings.DiskWarnPct    = [int]$v }
            'DiskCritPct'    { $Settings.DiskCritPct    = [int]$v }
            'MaxParallel'    { $Settings.MaxParallel    = [int]$v }
            default {}
          }
        }
      }
    }
    Save-Settings $Settings
    Refresh-Now
  }
}

# Persist column order on close
$Window.Add_Closing({
  try {
    $Settings.Window.Width  = [int]$Window.Width
    $Settings.Window.Height = [int]$Window.Height
    $Settings.ColumnOrder = @($Grid.Columns | Sort-Object DisplayIndex | ForEach-Object { $_.Header })
    Save-Settings $Settings
  } catch {}
})

# Wire events
$RefreshBtn.Add_Click({ Refresh-Now })
$RefreshMenu.Add_Click({ Refresh-Now })
$ExportBtn.Add_Click({ Export-CSV })
$ExportCsvMenu.Add_Click({ Export-CSV })

$ApplyAuto.Add_Click({
  $v = [int]$AutoRefBox.Text
  if ($v -gt 0) {
    $Settings.RefreshMinutes = $v
    Save-Settings $Settings
    $StatusText.Text = "Auto refresh set to $v min"
    $timer.Interval = [TimeSpan]::FromMinutes($v)
  }
})
$SettingsBtn.Add_Click({ Edit-Thresholds })
$SettingsMenu.Add_Click({ Edit-Thresholds })
$ExitMenu.Add_Click({ $Window.Close() })
$OpenServers.Add_Click({ Start-Process $ServerListPath })

# Timer
$timer = New-Object System.Windows.Threading.DispatcherTimer
$timer.Interval = [TimeSpan]::FromMinutes($Settings.RefreshMinutes)
$timer.Add_Tick({ Refresh-Now })
$timer.Start()
#endregion

#region --- Kickoff ---
# VB reference for InputBox
Add-Type -AssemblyName Microsoft.VisualBasic

Refresh-Now
$Window.ShowDialog() | Out-Null
#endregion
