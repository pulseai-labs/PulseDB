<#
.SYNOPSIS
    Bounded stack/region capture for ONE already-running diagnostic test process.

.DESCRIPTION
    Diagnostic-only helper for PR #88, run as a SEPARATE PROCESS by the probe so the
    probe can impose a wall-clock deadline and output-size caps on it from outside and
    kill it without delaying the test process's own termination.

    Run 34505038713 established what this exists to explain: the full-page test commits
    ~5.15 GiB/s and crossed 10.17 GiB private bytes in 2.1 s while its working set
    stayed at 38.7 MiB. Commit without touch. Nothing in that run says WHERE, because
    no stack or region evidence was taken. This captures both.

    Two artifacts, both small:

      * a stacks-and-modules minidump -- MiniDumpNormal | MiniDumpWithThreadInfo ONLY.
        Full-memory and private-memory flags are never passed and are not referenced
        anywhere in this file. For a handful of threads this is single-digit MB.
      * a virtual-memory REGION summary from VirtualQueryEx, aggregated by
        state x type x protection, plus the largest committed private regions. This is
        the artifact that names the allocation CLASS behind a 10 GiB private / 38 MiB
        resident split, and it is a few hundred KB at most.

    Scope, by construction: every call here operates on the single numeric PID handed
    down by the probe, opened with the least rights the three operations need. There is
    no process-name search, no enumeration, no system-wide capture, and no second PID.
    Descendants are NOT suspended and NOT dumped -- the probe's existing tree
    termination stays responsible for those, and this file never claims otherwise.

    Suspension is confirmed before any capture, and the target is never resumed:
    `NtResumeProcess` is not imported and not called, because the probe always
    terminates the target after this returns.

.PARAMETER TargetPid
    The numeric PID to capture. Passed down by the probe; never discovered here.

.PARAMETER OutputDirectory
    Directory to write capture artifacts into. The probe watches this directory's size
    while this process runs and kills it if a cap is breached.

.PARAMETER ResultPath
    Path for the structured JSON result.

.PARAMETER SymbolPath
    Optional path to the matching PDB, already validated by the probe.

.PARAMETER MaxFileBytes
    Advisory in-helper file cap. The probe enforces the same cap from outside, which is
    the authoritative one; this only avoids obviously pointless writes.

.PARAMETER SuspendSettleMs
    Delay between the two post-suspension counter reads used to prove the target is
    actually frozen.

.OUTPUTS
    Exit 0 - capture completed and the result JSON says so.
    Exit 3 - capture failed; the result JSON records why. The probe treats any non-zero
             exit, and any missing or invalid result JSON, as a capture failure.
#>

[CmdletBinding()]
param(
    # 0 is only legal together with -VerifyOnly, which captures nothing.
    [ValidateRange(0, 2147483647)]
    [int] $TargetPid = 0,

    [Parameter(Mandatory = $true)]
    [string] $OutputDirectory,

    [Parameter(Mandatory = $true)]
    [string] $ResultPath,

    # Compile the P/Invoke surface and report the three DLLs' paths and versions, then
    # exit without touching any process. The probe runs this BEFORE launching the
    # expensive test, so an Add-Type or tool problem costs seconds rather than a run.
    [switch] $VerifyOnly,

    [string] $SymbolPath = '',

    [ValidateRange(1048576, 268435456)]
    [long] $MaxFileBytes = 268435456,

    [ValidateRange(50, 2000)]
    [int] $SuspendSettleMs = 250
)

Set-StrictMode -Version 1.0
$ErrorActionPreference = 'Stop'
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

# Least rights for exactly the three operations performed here:
#   PROCESS_QUERY_INFORMATION (0x0400) - VirtualQueryEx and MiniDumpWriteDump
#   PROCESS_VM_READ           (0x0010) - MiniDumpWriteDump reading stacks
#   PROCESS_SUSPEND_RESUME    (0x0800) - NtSuspendProcess
# Deliberately NOT PROCESS_ALL_ACCESS, and deliberately no PROCESS_TERMINATE: this
# helper must not be able to kill anything.
$PROCESS_RIGHTS = 0x0410 -bor 0x0800

# MiniDumpNormal (0x0000) | MiniDumpWithThreadInfo (0x1000). Stacks, module list and
# thread info. No memory-bearing flag appears anywhere in this file.
$MINIDUMP_TYPE = 0x1000

$MEM_COMMIT = 0x1000
$MEM_RESERVE = 0x2000
$MEM_FREE = 0x10000
$MEM_PRIVATE = 0x20000
$MEM_MAPPED = 0x40000
$MEM_IMAGE = 0x1000000

$result = [ordered]@{
    schema              = 'pr88-windows-capture/1'
    target_pid          = $TargetPid
    started_utc         = (Get-Date).ToUniversalTime().ToString('o')
    ended_utc           = $null
    elapsed_s           = $null
    suspended           = $false
    suspend_method      = $null
    suspend_confirmed   = $false
    suspend_evidence    = $null
    counters_stable     = $false
    counters_evidence   = $null
    private_bytes_at_capture = $null
    working_set_at_capture   = $null
    dump                = $null
    regions             = $null
    symbols             = $null
    completed           = $false
    failure_reason      = $null
    failure_detail      = $null
}

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

if (-not $VerifyOnly -and $TargetPid -le 0) {
    throw 'TargetPid is required unless -VerifyOnly is given'
}

function Write-Result {
    param([string] $Reason, [string] $Detail, [bool] $Completed)
    $result.completed      = $Completed
    $result.failure_reason = $Reason
    $result.failure_detail = $Detail
    $result.ended_utc      = (Get-Date).ToUniversalTime().ToString('o')
    $result.elapsed_s      = [math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
    try {
        [pscustomobject]$result | ConvertTo-Json -Depth 8 |
            Set-Content -LiteralPath $ResultPath -Encoding utf8
    } catch {
        Write-Host "[capture] could not write result json: $($_.Exception.Message)"
    }
}

function Get-Sha256 {
    param([string] $Path)
    try { return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
    catch { return $null }
}

# ---------------------------------------------------------------------------
# P/Invoke, isolated to this file.
# ---------------------------------------------------------------------------
$typeSource = @'
using System;
using System.Runtime.InteropServices;

public static class Pr88Capture
{
    [StructLayout(LayoutKind.Sequential)]
    public struct MEMORY_BASIC_INFORMATION
    {
        public IntPtr BaseAddress;
        public IntPtr AllocationBase;
        public uint   AllocationProtect;
        public IntPtr RegionSize;
        public uint   State;
        public uint   Protect;
        public uint   Type;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern IntPtr OpenProcess(uint dwDesiredAccess, bool bInheritHandle, int dwProcessId);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool CloseHandle(IntPtr hObject);

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern IntPtr VirtualQueryEx(IntPtr hProcess, IntPtr lpAddress,
        out MEMORY_BASIC_INFORMATION lpBuffer, IntPtr dwLength);

    // Suspend only. NtResumeProcess is deliberately NOT imported: the probe always
    // terminates the target after this helper returns, so resuming is never correct.
    [DllImport("ntdll.dll", SetLastError = true)]
    public static extern int NtSuspendProcess(IntPtr processHandle);

    [DllImport("dbghelp.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool MiniDumpWriteDump(IntPtr hProcess, int processId, IntPtr hFile,
        int dumpType, IntPtr exceptionParam, IntPtr userStreamParam, IntPtr callbackParam);
}
'@

try {
    Add-Type -TypeDefinition $typeSource -Language CSharp -ErrorAction Stop
} catch {
    Write-Result -Reason 'pinvoke_compile_failed' -Detail $_.Exception.Message -Completed $false
    exit 3
}

$null = New-Item -ItemType Directory -Force -Path $OutputDirectory
$outDir = (Resolve-Path -LiteralPath $OutputDirectory).ProviderPath

if ($VerifyOnly) {
    # Path + version evidence for the three preinstalled DLLs this helper binds. No
    # download, no process is opened, nothing is captured.
    $tools = New-Object System.Collections.Generic.List[object]
    $missing = New-Object System.Collections.Generic.List[string]
    foreach ($dll in @('ntdll.dll', 'kernel32.dll', 'dbghelp.dll')) {
        $path = Join-Path $env:SystemRoot "System32\$dll"
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            $item = Get-Item -LiteralPath $path
            $tools.Add([pscustomobject]@{
                name    = $dll
                path    = $item.FullName
                bytes   = $item.Length
                version = [string]$item.VersionInfo.FileVersion
            })
        } else {
            $missing.Add("$dll not found at $path")
        }
    }
    $result.dump = $null
    $result.regions = $null
    $result.symbols = $null
    $result.suspend_method = 'n/a (verify only)'
    $result['verify_only'] = $true
    $result['pinvoke_compiled'] = $true
    $result['tools'] = @($tools)
    $result['powershell_version'] = $PSVersionTable.PSVersion.ToString()
    if ($missing.Count -gt 0) {
        Write-Result -Reason 'capture_tool_missing' -Detail ($missing -join '; ') -Completed $false
        exit 3
    }
    Write-Result -Reason $null -Detail $null -Completed $true
    exit 0
}

$handle = [IntPtr]::Zero
try {
    # ---- Open the one PID we were given, with least rights. --------------------
    $handle = [Pr88Capture]::OpenProcess($PROCESS_RIGHTS, $false, $TargetPid)
    if ($handle -eq [IntPtr]::Zero) {
        $err = [System.Runtime.InteropServices.Marshal]::GetLastWin32Error()
        Write-Result -Reason 'open_process_failed' -Detail "OpenProcess($TargetPid) failed, GetLastError=$err" -Completed $false
        exit 3
    }

    # ---- Suspend, then PROVE it took. -----------------------------------------
    # An unconfirmed suspension is treated as no suspension: the probe skips capture
    # and terminates immediately, because capturing a target that is still committing
    # 5 GiB/s is exactly the race this design exists to remove.
    $status = [Pr88Capture]::NtSuspendProcess($handle)
    $result.suspend_method = 'NtSuspendProcess'
    if ($status -ne 0) {
        Write-Result -Reason 'suspend_failed' -Detail "NtSuspendProcess returned NTSTATUS 0x$('{0:X8}' -f $status)" -Completed $false
        exit 3
    }
    $result.suspended = $true

    Start-Sleep -Milliseconds $SuspendSettleMs

    # Evidence 1: every thread reports a suspended wait.
    $threadStates = @()
    $suspendedThreads = 0
    $totalThreads = 0
    try {
        $p = Get-Process -Id $TargetPid -ErrorAction Stop
        $p.Refresh()
        foreach ($t in $p.Threads) {
            $totalThreads++
            $state = 'unknown'
            $reason = 'n/a'
            try {
                $state = [string]$t.ThreadState
                if ($state -eq 'Wait') { $reason = [string]$t.WaitReason }
            } catch {
                $reason = 'unreadable'
            }
            if ($reason -eq 'Suspended') { $suspendedThreads++ }
            $threadStates += "$($t.Id):$state/$reason"
        }
    } catch {
        Write-Result -Reason 'suspend_unconfirmed' -Detail "could not read thread states: $($_.Exception.Message)" -Completed $false
        exit 3
    }
    $result.suspend_evidence = "$suspendedThreads of $totalThreads threads report Wait/Suspended [$($threadStates -join '; ')]"
    if ($totalThreads -eq 0 -or $suspendedThreads -ne $totalThreads) {
        Write-Result -Reason 'suspend_unconfirmed' -Detail $result.suspend_evidence -Completed $false
        exit 3
    }
    $result.suspend_confirmed = $true

    # Evidence 2: the counters have actually stopped moving. A frozen process cannot
    # add commitment, which is what makes the capture deadline safe.
    $privA = $null; $privB = $null; $wsB = $null
    try {
        $p.Refresh(); $privA = [uint64]$p.PrivateMemorySize64
        Start-Sleep -Milliseconds $SuspendSettleMs
        $p.Refresh(); $privB = [uint64]$p.PrivateMemorySize64; $wsB = [uint64]$p.WorkingSet64
    } catch {
        Write-Result -Reason 'counters_unreadable' -Detail $_.Exception.Message -Completed $false
        exit 3
    }
    $delta = if ($privB -ge $privA) { $privB - $privA } else { $privA - $privB }
    $result.counters_evidence = "private bytes $privA -> $privB over $SuspendSettleMs ms (delta $delta B)"
    $result.private_bytes_at_capture = $privB
    $result.working_set_at_capture = $wsB
    # 1 MiB of slack absorbs measurement granularity; real growth here is ~5 GiB/s,
    # which would show as gigabytes over this interval, not megabytes.
    if ($delta -gt 1048576) {
        Write-Result -Reason 'counters_not_stable' -Detail $result.counters_evidence -Completed $false
        exit 3
    }
    $result.counters_stable = $true

    # ---- Stacks + modules minidump. -------------------------------------------
    $dumpPath = Join-Path $outDir "pid-$TargetPid.stacks.dmp"
    $fs = $null
    try {
        $fs = [System.IO.File]::Create($dumpPath)
        $ok = [Pr88Capture]::MiniDumpWriteDump($handle, $TargetPid, $fs.SafeFileHandle.DangerousGetHandle(),
                                               $MINIDUMP_TYPE, [IntPtr]::Zero, [IntPtr]::Zero, [IntPtr]::Zero)
        $lastErr = [System.Runtime.InteropServices.Marshal]::GetLastWin32Error()
    } finally {
        if ($null -ne $fs) { $fs.Dispose() }
    }
    if (-not $ok) {
        Write-Result -Reason 'minidump_failed' -Detail "MiniDumpWriteDump failed, GetLastError=$lastErr" -Completed $false
        exit 3
    }
    $dumpBytes = (Get-Item -LiteralPath $dumpPath).Length
    if ($dumpBytes -le 0) {
        Write-Result -Reason 'minidump_empty' -Detail "$dumpPath is $dumpBytes bytes" -Completed $false
        exit 3
    }
    if ($dumpBytes -gt $MaxFileBytes) {
        Remove-Item -LiteralPath $dumpPath -Force -ErrorAction SilentlyContinue
        Write-Result -Reason 'minidump_over_file_cap' -Detail "$dumpBytes B exceeds $MaxFileBytes B; file deleted" -Completed $false
        exit 3
    }
    $result.dump = [ordered]@{
        path        = Split-Path -Leaf $dumpPath
        bytes       = $dumpBytes
        sha256      = Get-Sha256 -Path $dumpPath
        dump_type   = "0x$('{0:X4}' -f $MINIDUMP_TYPE) (MiniDumpNormal|MiniDumpWithThreadInfo)"
        memory_flags_used = $false
    }

    # ---- Virtual-memory region summary. ---------------------------------------
    # Guarded against the two ways this walk can go wrong: a zero-length region, and
    # an address that fails to advance or wraps past the top of user space.
    $regionPath = Join-Path $outDir "pid-$TargetPid.regions.csv"
    $writer = New-Object System.IO.StreamWriter($regionPath, $false, [System.Text.UTF8Encoding]::new($false))
    $agg = @{}
    $largest = New-Object System.Collections.Generic.List[object]
    $regionCount = 0
    $truncated = $false
    try {
        $writer.WriteLine('base_address,allocation_base,region_size,state,type,protect,allocation_protect')
        $addr = [uint64]0
        $maxAddr = [uint64]0x7FFFFFFEFFFF
        $mbiSize = [IntPtr][System.Runtime.InteropServices.Marshal]::SizeOf([type][Pr88Capture+MEMORY_BASIC_INFORMATION])
        $mbi = New-Object 'Pr88Capture+MEMORY_BASIC_INFORMATION'
        while ($addr -lt $maxAddr) {
            if ($regionCount -ge 500000) { $truncated = $true; break }
            $written = [Pr88Capture]::VirtualQueryEx($handle, [IntPtr][int64]$addr, [ref] $mbi, $mbiSize)
            if ($written -eq [IntPtr]::Zero) { break }

            $size = [uint64]$mbi.RegionSize.ToInt64()
            if ($size -eq 0) { break }                      # zero-length guard

            $regionCount++
            $stateName   = switch ($mbi.State)  { $MEM_COMMIT { 'commit' } $MEM_RESERVE { 'reserve' } $MEM_FREE { 'free' } default { "0x$('{0:X}' -f $mbi.State)" } }
            $typeName    = switch ($mbi.Type)   { $MEM_PRIVATE { 'private' } $MEM_MAPPED { 'mapped' } $MEM_IMAGE { 'image' } default { "0x$('{0:X}' -f $mbi.Type)" } }
            $protectName = "0x$('{0:X}' -f $mbi.Protect)"

            $writer.WriteLine(('0x{0:X},0x{1:X},{2},{3},{4},{5},0x{6:X}' -f `
                $mbi.BaseAddress.ToInt64(), $mbi.AllocationBase.ToInt64(), $size,
                $stateName, $typeName, $protectName, $mbi.AllocationProtect))

            $key = "$stateName|$typeName|$protectName"
            if (-not $agg.ContainsKey($key)) { $agg[$key] = [pscustomobject]@{ state = $stateName; type = $typeName; protect = $protectName; regions = 0; bytes = [uint64]0 } }
            $agg[$key].regions++
            $agg[$key].bytes += $size

            if ($mbi.State -eq $MEM_COMMIT -and $mbi.Type -eq $MEM_PRIVATE) {
                $largest.Add([pscustomobject]@{ base = "0x$('{0:X}' -f $mbi.BaseAddress.ToInt64())"; bytes = $size; protect = $protectName })
            }

            $next = $addr + $size
            if ($next -le $addr) { $truncated = $true; break }   # no-advance / wraparound guard
            $addr = $next
        }
    } finally {
        $writer.Dispose()
    }

    $regionBytes = (Get-Item -LiteralPath $regionPath).Length
    if ($regionBytes -gt $MaxFileBytes) {
        Remove-Item -LiteralPath $regionPath -Force -ErrorAction SilentlyContinue
        Write-Result -Reason 'regions_over_file_cap' -Detail "$regionBytes B exceeds $MaxFileBytes B; file deleted" -Completed $false
        exit 3
    }

    $topPrivate = @($largest | Sort-Object -Property bytes -Descending | Select-Object -First 20)
    $result.regions = [ordered]@{
        path                = Split-Path -Leaf $regionPath
        bytes               = $regionBytes
        sha256              = Get-Sha256 -Path $regionPath
        region_count        = $regionCount
        walk_truncated      = $truncated
        by_state_type_protect = @($agg.Values | Sort-Object -Property bytes -Descending)
        largest_committed_private = $topPrivate
    }

    # ---- Symbols. The probe already proved a single matching PDB under cap. -----
    if ($SymbolPath -and (Test-Path -LiteralPath $SymbolPath -PathType Leaf)) {
        $pdb = Get-Item -LiteralPath $SymbolPath
        $result.symbols = [ordered]@{
            source_path = $pdb.FullName
            name        = $pdb.Name
            bytes       = $pdb.Length
            sha256      = Get-Sha256 -Path $pdb.FullName
            copied      = $false
            note        = 'authoritative dump-to-PDB linkage is the CodeView record inside the minidump module list; verify offline'
        }
        $copyTarget = Join-Path $outDir $pdb.Name
        if ($pdb.Length -le $MaxFileBytes) {
            try {
                Copy-Item -LiteralPath $pdb.FullName -Destination $copyTarget -Force
                $result.symbols.copied = $true
            } catch {
                $result.symbols.note = "copy failed: $($_.Exception.Message)"
            }
        }
    }

    Write-Result -Reason $null -Detail $null -Completed $true
    exit 0
}
catch {
    Write-Result -Reason 'capture_exception' -Detail "$($_.Exception.GetType().FullName): $($_.Exception.Message) at line $($_.InvocationInfo.ScriptLineNumber)" -Completed $false
    exit 3
}
finally {
    if ($handle -ne [IntPtr]::Zero) { $null = [Pr88Capture]::CloseHandle($handle) }
}
