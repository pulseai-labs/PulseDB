<#
.SYNOPSIS
    Bounded memory telemetry for two isolated sync-http integration tests on Windows.

.DESCRIPTION
    Diagnostic-only harness for PR #88. The Windows / sync-http CI job refused a
    3,105,296-byte allocation inside the sync_http test binary after 16 of 52 tests
    had completed (public run 34389089262, job 102593327289). The last COMPLETED test
    is not evidence of the culprit, and the Rust fast-fail message does not say which
    resource ran out.

    This script does not change, skip or shrink any test. It runs UNMODIFIED tests, one
    at a time, each as its own process, from a test binary that was already compiled,
    and samples memory counters while they run.

    Two distinctions this script is careful about:

      * Working set is resident pages; commit charge is backing-store reservation.
        They are different quantities, kept in separate columns, and neither is
        reported as the other. WHICH resource the failing run exhausted is NOT known.
        That is the open question this harness exists to gather evidence for, not an
        assumption it encodes.
      * A run stopped by one of the memory or time thresholds below is a DIAGNOSTIC
        LIMIT. A run stopped because the harness could no longer measure or control
        the child is a HARNESS FAILURE. Neither is a test assertion failure, and all
        three are classified apart in the summary.

    Timing limits, stated plainly rather than implied. Sampling is synchronous, so the
    watchdog is evaluated once per iteration, not continuously:

      * Elapsed time is tested at the TOP of every iteration, before any counter is
        read, so a slow sample cannot postpone the per-test timeout indefinitely.
      * Every CIM call carries -OperationTimeoutSec, so a single iteration cannot
        block forever waiting on the CIM service.
      * The per-test timeout can therefore overshoot by up to roughly one bounded
        iteration -- the CIM budget plus one process-counter read -- not by an
        unbounded amount. The workflow's job-level timeout is the outer bound.
      * A spike shorter than the sample interval can be missed entirely. These are
        peaks OBSERVED at the sampling frequency, not true maxima. Read them as
        lower bounds.

    Fail-closed policy: the FIRST unusable or lost system-telemetry sample ends the
    run. There is no window in which the child keeps running unmonitored. Likewise, a
    process-counter read that fails is treated as normal completion ONLY if the child's
    exit is positively confirmed; otherwise the child is terminated and the run is a
    harness failure.

    Termination is centralized in one function, and a REQUESTED kill is never reported
    as a completed one. The next test is not launched until the previous child's exit
    is confirmed; if it cannot be confirmed, the whole sequence aborts.

    Only the launched test process tree is ever terminated. The runner service and
    every unrelated process are out of scope by construction: the script kills by the
    PID it started -- captured while its handle is open, so the kernel cannot recycle
    it -- and its descendants, and nothing else.

    Everything sampled comes from built-in Windows surfaces (System.Diagnostics.Process
    and the Win32_PerfRawData_PerfOS_Memory CIM class). No profiler is installed, no
    memory is dumped, and no environment is captured.

.PARAMETER Executable
    Path to the already-built sync_http test binary.

.PARAMETER OutputDirectory
    Directory for CSV samples, per-test stdout/stderr and the JSON summary.

.PARAMETER Tests
    Exact test names to run, in order. Each runs with --exact --test-threads=1.

.PARAMETER SampleIntervalMs
    Sampling period in milliseconds.

.PARAMETER PerTestTimeoutSeconds
    Wall-clock budget per test process, after which its tree is terminated.

.PARAMETER PrivateBytesLimitGiB
    Terminate the test process tree when its private bytes exceed this.

.PARAMETER AvailableFloorGiB
    Terminate the test process tree when system available physical memory drops below this.

.PARAMETER CommitCeilingPercent
    Terminate the test process tree when system committed bytes exceed this share of the
    system commit limit.

.OUTPUTS
    Exit 0  - every test reached a recorded outcome (passed, failed, or stopped by a
              diagnostic limit). A failing or limit-stopped test is DATA, not an error.
    Exit 2  - harness failure: telemetry unavailable or lost, a child that could not be
              measured or confirmed terminated, a binary that could not be found, or a
              selection matching zero tests. Bounds are never silently dropped; the
              script refuses to start, or stops, instead.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $Executable,

    [Parameter(Mandatory = $true)]
    [string] $OutputDirectory,

    [Parameter(Mandatory = $true)]
    [string[]] $Tests,

    [ValidateRange(50, 10000)]
    [int] $SampleIntervalMs = 500,

    # Every range below is closed on the side that would WEAKEN the bound, so no
    # caller can raise the private-bytes cap, lower the available-memory floor,
    # raise the commit ceiling or extend the per-test timeout past the approved
    # limits. Tightening is allowed; loosening is a parameter-binding error.
    [ValidateRange(10, 180)]
    [int] $PerTestTimeoutSeconds = 180,

    [ValidateRange(0.5, 8)]
    [double] $PrivateBytesLimitGiB = 8,

    [ValidateRange(2, 64)]
    [double] $AvailableFloorGiB = 2,

    [ValidateRange(50, 90)]
    [double] $CommitCeilingPercent = 90,

    # ---- capture ------------------------------------------------------------
    # Every capture bound is closed on the loosening side, like the memory bounds.
    # Mandatory: this follow-up exists to capture, so a run without a helper is a
    # misconfiguration, not a quiet degrade to the previous behaviour.
    [Parameter(Mandatory = $true)]
    [string] $CaptureHelperPath,

    # Strictly below the private-bytes kill so capture happens with headroom. At the
    # observed ~5.15 GiB/s this is reached ~0.8 s in, with ~14 GiB of commit still
    # free -- and the target is suspended before anything is written, so growth stops
    # rather than racing the capture.
    [ValidateRange(0.5, 7.5)]
    [double] $CaptureTriggerGiB = 4,

    [ValidateRange(5, 60)]
    [int] $CaptureTimeoutSeconds = 60,

    [ValidateRange(1, 256)]
    [int] $CaptureMaxFileMiB = 256,

    [ValidateRange(1, 512)]
    [int] $CaptureMaxTotalMiB = 512
)

Set-StrictMode -Version 1.0
$ErrorActionPreference = 'Stop'

# PowerShell 7.4+ maps a non-zero native exit code onto $ErrorActionPreference.
# taskkill exits non-zero when the PID has already gone, which is a normal race
# here, not a harness failure. Native exit codes are checked explicitly instead.
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$GiB = 1073741824
$privateLimitBytes = [uint64]($PrivateBytesLimitGiB * $GiB)
$availableFloorBytes = [uint64]($AvailableFloorGiB * $GiB)

# Upper bound on any single CIM call, so one sample cannot stall the loop past the
# per-test timeout. A call that exceeds it throws, which the fail-closed policy
# treats as a lost sample.
$CimTimeoutSec = 5

# How long to wait for a terminated tree before declaring termination unconfirmed.
$TerminationWaitMs = 15000

$captureTriggerBytes = [uint64]($CaptureTriggerGiB * $GiB)
$captureMaxFileBytes = [int64]$CaptureMaxFileMiB * 1MB
$captureMaxTotalBytes = [int64]$CaptureMaxTotalMiB * 1MB
# Filled in by the pre-launch readiness check and reported in every result record.
$captureTools = $null
$captureSymbol = $null
$captureHelperFull = $null
$pwshPath = $null

function Write-Note {
    param([string] $Message)
    Write-Host "[probe] $Message"
}

function Read-SystemMemory {
    <#
        Win32_PerfRawData_PerfOS_Memory exposes AvailableBytes, CommittedBytes and
        CommitLimit as instantaneous gauges, so the raw values are usable directly --
        no rate computation, and no dependence on localized counter names.

        Throws on anything unusable, including a zero commit limit or zero available
        bytes. Callers treat a throw as a LOST SAMPLE and fail closed; there is no
        path that quietly returns a half-populated reading.
    #>
    $m = Get-CimInstance -ClassName Win32_PerfRawData_PerfOS_Memory `
                         -OperationTimeoutSec $script:CimTimeoutSec `
                         -ErrorAction Stop
    if ($null -eq $m) {
        throw 'Win32_PerfRawData_PerfOS_Memory returned no instance'
    }

    $limit = [uint64]$m.CommitLimit
    $committed = [uint64]$m.CommittedBytes
    $available = [uint64]$m.AvailableBytes

    if ($limit -le 0) {
        throw 'system commit limit read back as zero; the commit ceiling cannot be enforced'
    }
    if ($available -le 0) {
        throw 'system available bytes read back as zero; the available-memory floor cannot be enforced'
    }

    # CommitPercentRaw is the unrounded figure and is the ONLY one any threshold
    # compares against. CommitPercent is rounded for display: at 3 decimals a raw
    # 90.0004% rounds to 90.0 and would slip past a `-gt 90` test, so rounding must
    # never reach a comparison.
    $rawPercent = ($committed / $limit) * 100
    return [pscustomobject]@{
        AvailableBytes    = $available
        CommittedBytes    = $committed
        CommitLimitBytes  = $limit
        CommitPercentRaw  = $rawPercent
        CommitPercent     = [math]::Round($rawPercent, 3)
    }
}

function Test-LaunchEnvelope {
    <#
        The one place that decides whether it is safe to start a child process.

        Applies the same two system bounds the in-run watchdog applies -- available
        physical memory and system commit -- against UNROUNDED readings. Used before
        every child this script starts, the `--list` discovery child included: a
        runner already outside the envelope is not a safe place to start anything,
        and a discovery child is still a child.

        Returns $null when it is safe to launch, or a refusal object describing which
        bound was breached.
    #>
    param(
        [Parameter(Mandatory = $true)] $Reading,
        [Parameter(Mandatory = $true)] [string] $Stage
    )

    if ($Reading.AvailableBytes -lt $script:availableFloorBytes) {
        return [pscustomobject]@{
            Reason    = "${Stage}_available_memory_below_floor"
            Threshold = "$script:availableFloorBytes bytes ($script:AvailableFloorGiB GiB available physical)"
            Observed  = "$($Reading.AvailableBytes) bytes available before launch"
        }
    }
    if ($Reading.CommitPercentRaw -gt $script:CommitCeilingPercent) {
        return [pscustomobject]@{
            Reason    = "${Stage}_commit_over_ceiling"
            Threshold = "$script:CommitCeilingPercent% of a $($Reading.CommitLimitBytes)-byte commit limit"
            Observed  = "$($Reading.CommitPercentRaw)% raw ($($Reading.CommittedBytes) bytes committed) before launch"
        }
    }
    return $null
}

function Stop-TestTree {
    <#
        Terminates ONLY the tree rooted at the PID this script started, and reports
        whether exit was CONFIRMED.

        A requested kill is not a completed one. Every caller must branch on the
        returned boolean rather than assuming taskkill worked.
    #>
    param(
        [Parameter(Mandatory = $true)] $Process,
        [Parameter(Mandatory = $true)] [int] $ProcessId,
        [int] $WaitMilliseconds = 15000
    )

    $alreadyGone = $false
    try { $alreadyGone = $Process.HasExited } catch { $alreadyGone = $false }
    if ($alreadyGone) { return $true }

    try {
        $killOutput = & $script:taskkill '/PID' $ProcessId '/T' '/F' 2>&1
        foreach ($line in @($killOutput)) { Write-Note "  taskkill: $line" }
        Write-Note "  taskkill exit: $LASTEXITCODE"
    } catch {
        Write-Note "  taskkill raised: $($_.Exception.Message)"
    }

    try { $null = $Process.WaitForExit($WaitMilliseconds) } catch { }

    $confirmed = $false
    try { $confirmed = $Process.HasExited } catch { $confirmed = $false }
    return $confirmed
}

function New-RunRecord {
    <#
        The ONE place a per-run result object is constructed. The real loop and the
        self-check both call this, so the self-check exercises the actual conversion
        rather than an imitation of it. `[ordered]` in, so field order survives the
        call the way a literal `[pscustomobject]@{}` would.
    #>
    param(
        [Parameter(Mandatory = $true)]
        [System.Collections.Specialized.OrderedDictionary] $Fields
    )

    return [pscustomobject]$Fields
}

function Get-JsonEscaped {
    <#
        Minimal JSON string escaping for the fixed-shape fallback below. Deliberately
        uses only the -replace operator: ConvertTo-Json is one of the suspects the
        fallback exists to route around, so the fallback cannot depend on it.
    #>
    param([string] $Value)

    if ($null -eq $Value) { return '' }
    # Backslash first, then quote: reversing them would double-escape the backslash
    # this step just inserted. In a -replace replacement backslash is literal, so
    # each replacement below is written as the exact characters JSON should receive.
    $out = $Value -replace '\\', '\\'
    $out = $out -replace '"', '\"'
    $out = $out -replace "`r", '\r'
    $out = $out -replace "`n", '\n'
    $out = $out -replace "`t", '\t'
    # Anything else in the C0 range would make the file invalid JSON.
    $out = $out -replace '[\x00-\x1F]', ' '
    return $out
}

function Write-FallbackSummary {
    <#
        Writes a summary.json of fixed shape from string and integer literals only.

        This exists for exactly one situation: the self-check has just proved that the
        normal summary path throws. Routing the failure report back through
        Build-ProbeSummary / Write-ProbeSummary / ConvertTo-Json would then produce
        nothing at all, which is the failure mode that cost run 34505038713 its
        evidence. So this touches none of them.
    #>
    param(
        [Parameter(Mandatory = $true)] [string] $Path,
        [Parameter(Mandatory = $true)] [string] $Reason,
        [Parameter(Mandatory = $true)] [string] $Detail
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('{')
    $lines.Add('  "schema": "pr88-windows-memory-probe/fallback-1",')
    $lines.Add('  "generated_utc": "' + (Get-JsonEscaped ((Get-Date).ToUniversalTime().ToString('o'))) + '",')
    $lines.Add('  "fixed_shape_fallback": true,')
    $lines.Add('  "note": "the normal summary path failed its own pre-launch self-check; this file is built from literals only and uses neither Build-ProbeSummary nor ConvertTo-Json",')
    $lines.Add('  "sequence_aborted": true,')
    $lines.Add('  "tests_launched": 0,')
    $lines.Add('  "runs": [],')
    $lines.Add('  "failure_reason": "' + (Get-JsonEscaped $Reason) + '",')
    $lines.Add('  "failure_detail": "' + (Get-JsonEscaped $Detail) + '"')
    $lines.Add('}')
    ($lines -join "`n") | Set-Content -LiteralPath $Path -Encoding utf8
}

function Build-ProbeSummary {
    <#
        Builds the summary object from the SAME concrete collection types the real run
        uses. The parameters are typed to those exact generic types on purpose: run
        34505038713 died with `Argument types do not match` -- a reflection-level
        argument-binding failure -- somewhere in this construction, and a self-check
        that passed plain arrays through here would not exercise the binder that
        failed. See Invoke-ProbeSelfCheck.
    #>
    param(
        [Parameter(Mandatory = $true)] [System.Collections.Generic.List[object]] $Results,
        [Parameter(Mandatory = $true)] [System.Collections.Generic.List[string]] $HarnessFailures,
        [Parameter(Mandatory = $true)] [System.Collections.Generic.List[string]] $Skipped,
        [Parameter(Mandatory = $true)] [bool] $SequenceAborted
    )

    return [pscustomobject]@{
        schema             = 'pr88-windows-memory-probe/3'
        generated_utc      = (Get-Date).ToUniversalTime().ToString('o')
        executable         = $exePath
        bounds             = [pscustomobject]@{
            sample_interval_ms       = $SampleIntervalMs
            per_test_timeout_seconds = $PerTestTimeoutSeconds
            cim_operation_timeout_s  = $CimTimeoutSec
            termination_wait_ms      = $TerminationWaitMs
            private_bytes_limit      = $privateLimitBytes
            available_floor_bytes    = $availableFloorBytes
            commit_ceiling_percent   = $CommitCeilingPercent
        }
        timing_caveats     = @(
            'Sampling is synchronous: bounds are evaluated once per iteration, not continuously.',
            'Elapsed time is checked at the top of each iteration, before any counter read.',
            'The per-test timeout can overshoot by up to about one bounded iteration (the CIM timeout plus one process read).',
            'Peaks are the maxima OBSERVED at the sample interval, so they are lower bounds; a shorter spike can be missed.'
        )
        sequence_aborted   = $SequenceAborted
        tests_launched     = @($Results | Where-Object { $_.launched }).Count
        # Two different things share `launched = $false` and must never be summed
        # together: the harness correctly DECLINING to start a child on an unsafe
        # runner, and the harness FAILING before it could start one. The first is a
        # fact about the runner; the second is a defect in the run.
        tests_refused_pre_launch = @($Results | Where-Object { -not $_.launched -and $_.unlaunched_class -eq 'pre-launch-refusal' }).Count
        tests_unlaunched_harness_failure = @($Results | Where-Object { -not $_.launched -and $_.unlaunched_class -eq 'harness-failure' }).Count
        tests_not_run      = @($Skipped)
        harness_failures   = @($HarnessFailures)
        runs               = @($Results)
    }

}

function Build-StepSummaryLines {
    <#
        Renders the job-summary markdown. Split out for the same reason as
        Build-ProbeSummary: the self-check must cross this boundary too, since it is
        the other `-f`-heavy, collection-heavy path in the crash window.
    #>
    param(
        [Parameter(Mandatory = $true)] $Summary,
        [Parameter(Mandatory = $true)] [System.Collections.Generic.List[object]] $Results,
        [Parameter(Mandatory = $true)] [System.Collections.Generic.List[string]] $HarnessFailures,
        [Parameter(Mandatory = $true)] [System.Collections.Generic.List[string]] $Skipped,
        [Parameter(Mandatory = $true)] [bool] $SequenceAborted
    )

    $md = New-Object System.Collections.Generic.List[string]
        $md.Add('### Windows sync-http memory probe (PR #88, diagnostic only)')
        $md.Add('')
        $md.Add('| # | test | outcome | exit | elapsed s | peak private | peak working set | peak virtual | peak threads | min available | max commit % |')
        $md.Add('|---|------|---------|------|-----------|--------------|------------------|--------------|--------------|---------------|--------------|')
        foreach ($run in $Results) {
            $md.Add(('| {0} | `{1}` | {2} | {3} | {4} | {5:N0} | {6:N0} | {7:N0} | {8} | {9:N0} | {10} |' -f `
                $run.index, $run.test, $run.outcome, $run.exit_code, $run.elapsed_seconds,
                $run.peak_process.private_bytes, $run.peak_process.working_set_bytes,
                $run.peak_process.virtual_bytes, $run.peak_process.thread_count,
                $run.system_extremes.min_available_bytes, $run.system_extremes.max_commit_percent))
        }
        $md.Add('')
        $md.Add('Byte figures are bytes. Private bytes and working set are different quantities and are not interchangeable. Which resource the original CI failure exhausted is not established by this run.')
        $md.Add('Peaks are maxima observed at the sample interval, so they are lower bounds.')
        foreach ($run in $Results) {
            # `stop_class` is the authority here, exactly as it is in the JSON and in
            # harness_failures. `stopped_by_watchdog` records the historical fact that
            # a bound fired; it must never decide how the effective result is
            # described, or a run whose evidence is incomplete gets headlined as a
            # clean diagnostic limit.
            if ($run.launched -and $run.stop_class -eq 'diagnostic-limit') {
                $md.Add(('- `{0}` was stopped by a DIAGNOSTIC LIMIT (`{1}`; threshold {2}, observed {3}){4}. This is a harness bound, not a test assertion failure.' -f `
                    $run.test, $run.stop_reason, $run.stop_threshold, $run.stop_observed,
                    $(if ($run.stopped_by_watchdog) { ' and its process tree is confirmed gone' } else { '' })))
            } elseif ($run.launched -and $run.stop_class -eq 'harness-failure') {
                $md.Add(('- `{0}` stopped on a HARNESS FAILURE (`{1}`: {2}). This says nothing about the test.' -f `
                    $run.test, $run.stop_reason, $run.stop_observed))
                if ($run.stop_reason_secondary) {
                    # Secondary evidence, with its OWN threshold and observation. The
                    # earlier bound is reported, not promoted: the effective result is
                    # still the harness failure above.
                    $md.Add(('  - secondary evidence: an earlier stop was recorded first (`{0}`; threshold {1}, observed {2}){3}. It is subordinate to the effective reason above and does not reclassify this run.' -f `
                        $run.stop_reason_secondary, $run.stop_threshold_secondary, $run.stop_observed_secondary,
                        $(if ($run.stopped_by_watchdog) { ', and its process tree is confirmed gone' } else { '' })))
                } elseif ($run.stopped_by_watchdog) {
                    $md.Add('  - secondary evidence: a diagnostic bound fired and the process tree is confirmed gone, but the effective result is the harness failure above.')
                }
            }
            # An unlaunched run is described by the NOT LAUNCHED lines below and by
            # those alone, so nothing is stated twice.
            if (-not $run.launched -and $run.unlaunched_class -eq 'pre-launch-refusal') {
                $md.Add(('- `{0}` was NOT LAUNCHED — pre-launch resource refusal: the runner was already outside the safe envelope before the child started (`{1}`; threshold {2}, observed {3}). The harness worked correctly; no memory figures exist for this test.' -f `
                    $run.test, $run.stop_reason, $run.stop_threshold, $run.stop_observed))
            } elseif (-not $run.launched) {
                $md.Add(('- `{0}` was NOT LAUNCHED — HARNESS FAILURE before the child could start (`{1}`: {2}). This is a defect in the run, not a fact about the runner.' -f `
                    $run.test, $run.stop_reason, $run.stop_observed))
            }
            if ($run.writer_dispose_error) {
                $md.Add(('- `{0}`: the samples file FAILED TO CLOSE (`{1}`). The CSV for this run may be truncated, so its telemetry is not trustworthy — recorded as a harness failure regardless of why the child stopped{2}.' -f `
                    $run.test, $run.writer_dispose_error,
                    $(if ($run.stop_reason_secondary) { " (it had already stopped for ``$($run.stop_reason_secondary)``)" } else { '' })))
            }
            if ($run.termination_requested -and -not $run.termination_confirmed) {
                $md.Add(('- `{0}`: termination of PID {1} was REQUESTED but NOT CONFIRMED.' -f $run.test, $run.pid))
            }
        }
        if ($SequenceAborted) {
            $md.Add(('- SEQUENCE ABORTED. Tests not run: {0}' -f (@($Skipped) -join ', ')))
        }
        foreach ($f in $HarnessFailures) { $md.Add("- HARNESS FAILURE: $f") }
    return $md
}

function Write-ProbeSummary {
    <#
        The real serialization and write. The self-check calls THIS, against a
        throwaway path, so a failure in ConvertTo-Json or Set-Content is caught before
        the expensive test rather than after it.
    #>
    param(
        [Parameter(Mandatory = $true)] $Summary,
        [Parameter(Mandatory = $true)] [string] $Path
    )

    $Summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Path -Encoding utf8
}

function Invoke-BoundedCapture {
    <#
        Drives the capture helper as a SEPARATE PROCESS and enforces every bound from
        outside it.

        The helper cannot be trusted to bound itself: a helper that hangs, or that
        writes without limit, is exactly what the caps exist for. So the parent owns
        the deadline and both size caps and polls them WHILE the helper writes --
        checking sizes only after the fact would not be a cap at all.

        Two distinct process trees are in play and they are never confused: this kills
        the HELPER tree on a breach. The captured TEST tree is terminated by the
        probe's existing unconditional cleanup path, which this function neither
        performs nor delays beyond its own hard deadline.

        Returns a reporting object; never throws.
    #>
    param(
        [Parameter(Mandatory = $true)] [int] $ProcessId,
        [Parameter(Mandatory = $true)] [uint64] $PrivateBytes,
        [Parameter(Mandatory = $true)] [string] $CaptureDirectory
    )

    $state = [ordered]@{
        requested             = $true
        armed                 = $true
        trigger_reason        = 'process_private_bytes_over_capture_trigger'
        trigger_private_bytes = $PrivateBytes
        tool                  = 'dbghelp.dll MiniDumpWriteDump (MiniDumpNormal|MiniDumpWithThreadInfo)'
        tool_version          = $null
        target_pid            = $ProcessId
        scope_note            = 'root test PID only; descendants are neither suspended nor dumped'
        suspended             = $false
        suspend_method        = $null
        suspend_confirmed     = $false
        counters_stable       = $false
        completed             = $false
        elapsed_s             = $null
        helper_exit_code      = $null
        files                 = @()
        total_bytes           = [int64]0
        symbols               = $script:captureSymbol
        failure_reason        = $null
        failure_detail        = $null
    }
    if ($null -ne $script:captureTools) {
        $dbg = @($script:captureTools | Where-Object { $_.name -eq 'dbghelp.dll' })
        if ($dbg.Count -eq 1) { $state.tool_version = $dbg[0].version }
    }

    $null = New-Item -ItemType Directory -Force -Path $CaptureDirectory
    $capDir = (Resolve-Path -LiteralPath $CaptureDirectory).ProviderPath
    $resultPath = Join-Path $capDir 'capture-result.json'
    $helperOut = Join-Path $capDir 'capture-helper.stdout.txt'
    $helperErr = Join-Path $capDir 'capture-helper.stderr.txt'

    $helper = $null
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $helperArgs = @(
            '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
            '-File', $script:captureHelperFull,
            '-TargetPid', $ProcessId,
            '-OutputDirectory', $capDir,
            '-ResultPath', $resultPath,
            '-MaxFileBytes', $script:captureMaxFileBytes
        )
        if ($script:captureSymbol -and $script:captureSymbol.path) {
            $helperArgs += @('-SymbolPath', $script:captureSymbol.path)
        }
        $helper = Start-Process -FilePath $script:pwshPath -ArgumentList $helperArgs `
                                -NoNewWindow -PassThru `
                                -RedirectStandardOutput $helperOut -RedirectStandardError $helperErr
    } catch {
        $state.failure_reason = 'capture_helper_launch_failed'
        $state.failure_detail = $_.Exception.Message
        $state.elapsed_s = [math]::Round($sw.Elapsed.TotalSeconds, 3)
        return [pscustomobject]$state
    }

    $helperPid = $helper.Id
    $breach = $null
    $breachDetail = $null

    while ($true) {
        $exited = $true
        try { $exited = $helper.HasExited } catch { $exited = $true }
        if ($exited) { break }

        if ($sw.Elapsed.TotalSeconds -gt $script:CaptureTimeoutSeconds) {
            $breach = 'capture_timeout'
            $breachDetail = "helper exceeded $($script:CaptureTimeoutSeconds) s"
        } else {
            # Live size enforcement, not a post-write audit.
            $total = [int64]0
            foreach ($f in @(Get-ChildItem -LiteralPath $capDir -File -ErrorAction SilentlyContinue)) {
                if ($f.Length -gt $script:captureMaxFileBytes) {
                    $breach = 'capture_file_cap_exceeded'
                    $breachDetail = "$($f.Name) reached $($f.Length) B, cap $($script:captureMaxFileBytes) B"
                    break
                }
                $total += $f.Length
            }
            if ($null -eq $breach -and $total -gt $script:captureMaxTotalBytes) {
                $breach = 'capture_total_cap_exceeded'
                $breachDetail = "$total B across the capture directory, cap $($script:captureMaxTotalBytes) B"
            }
        }

        if ($null -ne $breach) {
            Write-Host "::warning::capture bound breached: $breach ($breachDetail); killing the capture helper"
            try {
                $out = & $script:taskkill '/PID' $helperPid '/T' '/F' 2>&1
                foreach ($line in @($out)) { Write-Note "  capture taskkill: $line" }
            } catch {
                Write-Note "  capture taskkill raised: $($_.Exception.Message)"
            }
            try { $null = $helper.WaitForExit(10000) } catch { }
            break
        }

        Start-Sleep -Milliseconds 250
    }

    try { $null = $helper.WaitForExit(10000) } catch { }
    $sw.Stop()
    $state.elapsed_s = [math]::Round($sw.Elapsed.TotalSeconds, 3)
    try { $state.helper_exit_code = $helper.ExitCode } catch { $state.helper_exit_code = $null }

    # Whatever was written stays: partial evidence is preserved, never cleaned up.
    $files = New-Object System.Collections.Generic.List[object]
    $total = [int64]0
    foreach ($f in @(Get-ChildItem -LiteralPath $capDir -File -ErrorAction SilentlyContinue)) {
        $sha = $null
        try { $sha = (Get-FileHash -LiteralPath $f.FullName -Algorithm SHA256).Hash.ToLowerInvariant() } catch { }
        $files.Add([pscustomobject]@{ name = $f.Name; bytes = $f.Length; sha256 = $sha })
        $total += $f.Length
    }
    $state.files = @($files)
    $state.total_bytes = $total

    if ($null -ne $breach) {
        $state.failure_reason = $breach
        $state.failure_detail = $breachDetail
        return [pscustomobject]$state
    }

    if (-not (Test-Path -LiteralPath $resultPath -PathType Leaf)) {
        $state.failure_reason = 'capture_result_missing'
        $state.failure_detail = "helper exited $($state.helper_exit_code) without writing $resultPath"
        return [pscustomobject]$state
    }

    $helperResult = $null
    try {
        $helperResult = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
    } catch {
        $state.failure_reason = 'capture_result_invalid'
        $state.failure_detail = $_.Exception.Message
        return [pscustomobject]$state
    }

    $state.suspended         = [bool]$helperResult.suspended
    $state.suspend_method    = $helperResult.suspend_method
    $state.suspend_confirmed = [bool]$helperResult.suspend_confirmed
    $state.counters_stable   = [bool]$helperResult.counters_stable

    if ($state.helper_exit_code -ne 0 -or -not $helperResult.completed) {
        $state.failure_reason = if ($helperResult.failure_reason) { $helperResult.failure_reason } else { 'capture_helper_failed' }
        $state.failure_detail = $helperResult.failure_detail
        return [pscustomobject]$state
    }
    if (-not $state.suspend_confirmed -or -not $state.counters_stable) {
        $state.failure_reason = 'suspend_unconfirmed'
        $state.failure_detail = "$($helperResult.suspend_evidence) | $($helperResult.counters_evidence)"
        return [pscustomobject]$state
    }

    $state.completed = $true
    return [pscustomobject]$state
}

function Invoke-ProbeSelfCheck {
    <#
        Proves the summary path works BEFORE the expensive test runs.

        Run 34505038713 measured both tests correctly and then died with an unhandled
        `Argument types do not match` -- a reflection-level argument-binding failure --
        while building or serializing the summary, losing summary.json and the job
        summary entirely. Static reading of the crash window did not isolate the
        throwing expression, so this does not guess at it: it runs the real code and
        reports the line.

        To be able to falsify a collection-binder failure it must use the SAME concrete
        types the real run uses -- List[object] for results, List[string] for the two
        string collections -- build records through New-RunRecord, and cross the real
        Build-ProbeSummary / Build-StepSummaryLines / Write-ProbeSummary boundary
        against a throwaway path. Synthetic arrays passed only through helpers would
        prove nothing about the binder that actually failed.

        Covers every record shape the loop can emit, with capture fields both populated
        and null.

        Returns $null on success, or a detail string naming the exception type, line
        and stack.
    #>
    param([Parameter(Mandatory = $true)] [string] $ScratchPath)

    try {
        $probeResults = New-Object System.Collections.Generic.List[object]
        $probeFailures = New-Object System.Collections.Generic.List[string]
        $probeSkipped = New-Object System.Collections.Generic.List[string]

        $syntheticCapture = [pscustomobject]@{
            requested = $true; armed = $true; completed = $true
            trigger_reason = 'process_private_bytes_over_capture_trigger'
            trigger_private_bytes = [uint64]4294967296
            tool = 'dbghelp.dll MiniDumpWriteDump'; tool_version = '10.0.26100.1'
            suspended = $true; suspend_method = 'NtSuspendProcess'; suspend_confirmed = $true
            elapsed_s = 1.234; total_bytes = [int64]5242880
            files = @([pscustomobject]@{ name = 'pid-1.stacks.dmp'; bytes = [int64]5242880; sha256 = ('0' * 64) })
            failure_reason = $null
        }

        # 1: launched, completed cleanly, no capture.
        $probeResults.Add((New-RunRecord -Fields ([ordered]@{
            index = 1; test = 'selfcheck-completed'; command = 'x'; executable = 'x'
            arguments = @('a', '--exact'); launched = $true; unlaunched_class = $null
            pid = 1234; started_utc = '2026-01-01T00:00:00.0000000Z'; ended_utc = '2026-01-01T00:00:02.0000000Z'
            elapsed_seconds = 2.0; exit_code = 0; outcome = 'process-completed'
            stopped_by_watchdog = $false; stop_reason = $null; stop_reason_secondary = $null
            stop_threshold_secondary = $null; stop_observed_secondary = $null; stop_class = $null
            stop_threshold = $null; stop_observed = $null; stop_detail = $null
            writer_dispose_error = $null; stop_sample = $null
            termination_requested = $false; termination_confirmed = $true
            libtest = [pscustomobject]@{ running_count = 1; result_line = 'test result: ok. 1 passed'; passed = 1; failed = 0; ignored = 0; zero_tests_selected = $false }
            baseline_system = [pscustomobject]@{ available_bytes = [uint64]1; committed_bytes = [uint64]2; commit_limit_bytes = [uint64]3; commit_percent = 66.667 }
            peak_process = [pscustomobject]@{ private_bytes = [uint64]150433792; working_set_bytes = [uint64]41013248; virtual_bytes = [uint64]4531113984; thread_count = 6; handle_count = 88 }
            system_extremes = [pscustomobject]@{ min_available_bytes = [uint64]14125879296; max_committed_bytes = [uint64]2930601984; max_commit_percent = 14.464; max_commit_percent_raw = 14.4642871 }
            sample_count = 4; sample_interval_ms = 500; slowest_sample_s = 0.02
            capture = $null
            stdout_file = 'a.stdout.txt'; stderr_file = 'a.stderr.txt'; samples_file = 'a.samples.csv'
        })))

        # 2: launched, stopped by a diagnostic bound, capture completed.
        $probeResults.Add((New-RunRecord -Fields ([ordered]@{
            index = 2; test = 'selfcheck-diagnostic-limit'; command = 'x'; executable = 'x'
            arguments = @('a', '--exact'); launched = $true; unlaunched_class = $null
            pid = 5678; started_utc = '2026-01-01T00:00:00.0000000Z'; ended_utc = '2026-01-01T00:00:02.1000000Z'
            elapsed_seconds = 2.122; exit_code = 1; outcome = 'diagnostic-limit'
            stopped_by_watchdog = $true; stop_reason = 'process_private_bytes_over_capture_trigger'
            stop_reason_secondary = $null; stop_threshold_secondary = $null; stop_observed_secondary = $null
            stop_class = 'diagnostic-limit'; stop_threshold = '4294967296 bytes'; stop_observed = '10925547520 bytes private'
            stop_detail = 'captured then terminated'; writer_dispose_error = $null; stop_sample = 5
            termination_requested = $true; termination_confirmed = $true
            libtest = [pscustomobject]@{ running_count = 1; result_line = $null; passed = $null; failed = $null; ignored = $null; zero_tests_selected = $false }
            baseline_system = [pscustomobject]@{ available_bytes = [uint64]14166269952; committed_bytes = [uint64]2765869056; commit_limit_bytes = [uint64]20261367808; commit_percent = 13.651 }
            peak_process = [pscustomobject]@{ private_bytes = [uint64]10925547520; working_set_bytes = [uint64]40591360; virtual_bytes = [uint64]15439839232; thread_count = 5; handle_count = 82 }
            system_extremes = [pscustomobject]@{ min_available_bytes = [uint64]14112043008; max_committed_bytes = [uint64]13762150400; max_commit_percent = 67.923; max_commit_percent_raw = 67.9231122 }
            sample_count = 5; sample_interval_ms = 500; slowest_sample_s = 0.022
            capture = $syntheticCapture
            stdout_file = 'b.stdout.txt'; stderr_file = 'b.stderr.txt'; samples_file = 'b.samples.csv'
        })))

        # 3: launched, harness failure with a populated secondary triplet and a
        #    failed capture -- the densest shape the loop can produce.
        $probeResults.Add((New-RunRecord -Fields ([ordered]@{
            index = 3; test = 'selfcheck-harness-failure'; command = 'x'; executable = 'x'
            arguments = @('a', '--exact'); launched = $true; unlaunched_class = $null
            pid = 9012; started_utc = '2026-01-01T00:00:00.0000000Z'; ended_utc = '2026-01-01T00:00:03.0000000Z'
            elapsed_seconds = 3.0; exit_code = 1; outcome = 'harness-failure'
            stopped_by_watchdog = $false; stop_reason = 'termination_unconfirmed'
            stop_reason_secondary = 'process_private_bytes_over_limit'
            stop_threshold_secondary = '8589934592 bytes'; stop_observed_secondary = '10925547520 bytes private'
            stop_class = 'harness-failure'; stop_threshold = 'tree must be confirmed gone'
            stop_observed = 'PID 9012 not confirmed within 15000 ms'; stop_detail = 'live child'
            writer_dispose_error = 'the process cannot access the file'; stop_sample = 5
            termination_requested = $true; termination_confirmed = $false
            libtest = [pscustomobject]@{ running_count = 1; result_line = $null; passed = $null; failed = $null; ignored = $null; zero_tests_selected = $false }
            baseline_system = [pscustomobject]@{ available_bytes = [uint64]1; committed_bytes = [uint64]2; commit_limit_bytes = [uint64]3; commit_percent = 66.667 }
            peak_process = [pscustomobject]@{ private_bytes = [uint64]10925547520; working_set_bytes = [uint64]40591360; virtual_bytes = [uint64]15439839232; thread_count = 5; handle_count = 82 }
            system_extremes = [pscustomobject]@{ min_available_bytes = [uint64]1; max_committed_bytes = [uint64]2; max_commit_percent = 3.0; max_commit_percent_raw = 3.00001 }
            sample_count = 5; sample_interval_ms = 500; slowest_sample_s = 0.9
            capture = [pscustomobject]@{ requested = $true; armed = $true; completed = $false; trigger_reason = 'process_private_bytes_over_capture_trigger'; trigger_private_bytes = [uint64]4294967296; tool = 'dbghelp.dll MiniDumpWriteDump'; tool_version = '10.0.26100.1'; suspended = $false; suspend_method = 'NtSuspendProcess'; suspend_confirmed = $false; elapsed_s = 0.4; total_bytes = [int64]0; files = @(); failure_reason = 'suspend_unconfirmed' }
            stdout_file = 'c.stdout.txt'; stderr_file = 'c.stderr.txt'; samples_file = 'c.samples.csv'
        })))

        # 4 and 5: the two unlaunched shapes, which carry nulls where the launched
        #    shapes carry objects.
        foreach ($pair in @(@('selfcheck-pre-launch-refusal', 'pre-launch-refusal', 'diagnostic-limit'),
                            @('selfcheck-unlaunched-harness', 'harness-failure', 'harness-failure'))) {
            $probeResults.Add((New-RunRecord -Fields ([ordered]@{
                index = 0; test = $pair[0]; command = 'x'; executable = 'x'
                arguments = @('a', '--exact'); launched = $false; unlaunched_class = $pair[1]
                pid = $null; started_utc = $null; ended_utc = $null; elapsed_seconds = $null
                exit_code = $null; outcome = $pair[2]; stopped_by_watchdog = $false
                stop_reason = 'pre_launch_commit_over_ceiling'; stop_reason_secondary = $null
                stop_threshold_secondary = $null; stop_observed_secondary = $null
                stop_class = $pair[2]; stop_threshold = '90% of a 20261367808-byte commit limit'
                stop_observed = '91.5% raw'; stop_detail = 'nothing was started'
                writer_dispose_error = $null; stop_sample = $null
                termination_requested = $false; termination_confirmed = $true
                libtest = $null; baseline_system = $null; peak_process = $null; system_extremes = $null
                sample_count = 0; sample_interval_ms = 500; slowest_sample_s = $null
                capture = $null; stdout_file = $null; stderr_file = $null; samples_file = $null
            })))
        }

        $probeFailures.Add('selfcheck synthetic harness failure entry')
        $probeSkipped.Add('selfcheck-skipped-test')

        $summary = Build-ProbeSummary -Results $probeResults -HarnessFailures $probeFailures `
                                      -Skipped $probeSkipped -SequenceAborted $true
        Write-ProbeSummary -Summary $summary -Path $ScratchPath
        $lines = Build-StepSummaryLines -Summary $summary -Results $probeResults `
                                        -HarnessFailures $probeFailures -Skipped $probeSkipped `
                                        -SequenceAborted $true
        $null = ($lines -join "`n")

        if (-not (Test-Path -LiteralPath $ScratchPath -PathType Leaf)) {
            return 'self-check wrote no file'
        }
        if ((Get-Item -LiteralPath $ScratchPath).Length -le 0) {
            return 'self-check wrote a zero-byte summary'
        }
        return $null
    } catch {
        return ("{0}: {1} | line {2} | statement: {3} | stack: {4}" -f `
            $_.Exception.GetType().FullName, $_.Exception.Message,
            $_.InvocationInfo.ScriptLineNumber, ($_.InvocationInfo.Line).Trim(), $_.ScriptStackTrace)
    }
}

function Complete-Probe {
    <#
        Writes summary.json and the job summary, then exits with the code the run
        earned. EVERY terminating path after the output directory exists goes through
        here, so an abort can never leave the artifact without a summary: losing the
        JSON is losing the evidence the whole run exists to produce.
    #>
    $summaryPath = Join-Path $outDir 'summary.json'
    $summary = Build-ProbeSummary -Results $script:results `
                                  -HarnessFailures $script:harnessFailures `
                                  -Skipped $script:skipped `
                                  -SequenceAborted $script:sequenceAborted
    Write-ProbeSummary -Summary $summary -Path $summaryPath
    Write-Note "summary written to $summaryPath"

    if ($env:GITHUB_STEP_SUMMARY) {
        $md = Build-StepSummaryLines -Summary $summary `
                                     -Results $script:results `
                                     -HarnessFailures $script:harnessFailures `
                                     -Skipped $script:skipped `
                                     -SequenceAborted $script:sequenceAborted
        ($md -join "`n") | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Encoding utf8
    }

    if ($script:harnessFailures.Count -gt 0 -or $script:sequenceAborted) { exit 2 }
    exit 0
}

# ---------------------------------------------------------------------------
# Output directory and accumulators come FIRST, before any check that can fail.
# Complete-Probe needs both, and a run that dies without summary.json has thrown
# away the evidence it exists to produce.
# ---------------------------------------------------------------------------

$null = New-Item -ItemType Directory -Force -Path $OutputDirectory
$outDir = (Resolve-Path -LiteralPath $OutputDirectory).ProviderPath

# The raw parameter until preflight proves the file exists; resolved to a full
# path immediately afterwards. Declared now so Complete-Probe can always read it.
$exePath = $Executable

# Stop reasons that mean "the experiment hit a bound we set". Anything else that
# stops a run means the harness lost the ability to measure or control the child,
# which is a harness failure, not a datapoint about the test.
$diagnosticLimitReasons = @(
    'discovery_pre_launch_available_memory_below_floor',
    'discovery_pre_launch_commit_over_ceiling',
    'pre_launch_available_memory_below_floor',
    'pre_launch_commit_over_ceiling',
    'process_private_bytes_over_capture_trigger',
    'process_private_bytes_over_limit',
    'system_available_memory_below_floor',
    'system_commit_over_ceiling',
    'per_test_timeout'
)

$results = New-Object System.Collections.Generic.List[object]
$harnessFailures = New-Object System.Collections.Generic.List[string]
$skipped = New-Object System.Collections.Generic.List[string]
$sequenceAborted = $false
$index = 0

# ---------------------------------------------------------------------------
# Preflight. If bounded telemetry cannot be implemented with built-in APIs, the
# script STOPS here rather than running the tests without a watchdog.
# ---------------------------------------------------------------------------

$preflightErrors = New-Object System.Collections.Generic.List[string]

if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
    $preflightErrors.Add("test binary not found at '$Executable'")
}

$taskkill = Join-Path $env:SystemRoot 'System32\taskkill.exe'
if (-not (Test-Path -LiteralPath $taskkill -PathType Leaf)) {
    $preflightErrors.Add("taskkill.exe not found at '$taskkill'; the process-tree watchdog cannot be armed")
}

# Read-SystemMemory validates the commit limit and available bytes itself, so this
# single call proves the whole system-side bound is enforceable.
try {
    $null = Read-SystemMemory
} catch {
    $preflightErrors.Add("system memory telemetry is unusable: $($_.Exception.Message)")
}

try {
    $self = Get-Process -Id $PID -ErrorAction Stop
    $self.Refresh()
    if ($self.PrivateMemorySize64 -le 0 -or $self.VirtualMemorySize64 -le 0) {
        $preflightErrors.Add('per-process memory counters read back as zero; the private-bytes limit cannot be enforced')
    }
} catch {
    $preflightErrors.Add("per-process counters are unreadable: $($_.Exception.Message)")
}

if ($preflightErrors.Count -gt 0) {
    Write-Host '::error::bounded telemetry is unavailable; refusing to run the tests without a watchdog'
    foreach ($e in $preflightErrors) { Write-Host "::error::$e" }
    # Controlled exit through Complete-Probe, not a bare `exit`: a telemetry failure
    # BEFORE the tests is classified and written to summary.json exactly like one
    # BETWEEN them.
    foreach ($e in $preflightErrors) { $harnessFailures.Add("preflight: $e") }
    $sequenceAborted = $true
    foreach ($t in $Tests) { $skipped.Add($t) }
    Complete-Probe
}

$exePath = (Resolve-Path -LiteralPath $Executable).ProviderPath

# `--list` executes no test, but it IS a child process, and starting any child on a
# runner already outside the safe envelope is the thing the gate exists to prevent.
# Each discovery child is therefore gated by its OWN fresh reading, taken
# immediately before it launches -- not by one baseline sampled before the loop.
# Memory and commit move while the loop runs, so a single up-front verdict says
# nothing about the second child.
#
# A misspelled test name would otherwise cost a whole run to discover, so the loop
# itself is worth keeping cheap: `--list` enumerates and exits.
$selectionErrors = New-Object System.Collections.Generic.List[string]
$discoveryIndex = 0

foreach ($t in $Tests) {
    $discoveryIndex++

    # ---- Fresh gate, immediately before THIS child. ---------------------------
    $discoveryReading = $null
    $discoveryTelemetryError = $null
    try {
        $discoveryReading = Read-SystemMemory
    } catch {
        $discoveryTelemetryError = $_.Exception.Message
    }

    if ($null -eq $discoveryReading) {
        $msg = "system memory telemetry became unusable before the discovery child for '$t': $discoveryTelemetryError"
        Write-Host "::error::$msg"
        $harnessFailures.Add($msg)
        $results.Add((New-RunRecord -Fields ([ordered]@{
            index                 = 0
            test                  = "(test discovery: $t)"
            command               = ('"{0}" {1} --exact --list' -f $exePath, $t)
            executable            = $exePath
            arguments             = @($t, '--exact', '--list')
            launched              = $false
            # A defect in the run, not a fact about the runner: no safety verdict
            # could be reached, so the gate never got to allow or refuse.
            unlaunched_class      = 'harness-failure'
            pid                   = $null
            started_utc           = $null
            ended_utc             = $null
            elapsed_seconds       = $null
            exit_code             = $null
            outcome               = 'harness-failure'
            stopped_by_watchdog   = $false
            stop_reason           = 'discovery_baseline_system_telemetry_lost'
            stop_class            = 'harness-failure'
            stop_threshold        = 'system memory telemetry must be readable before every child, discovery included'
            stop_observed         = $discoveryTelemetryError
            stop_detail           = 'no safety verdict could be reached, so the discovery child was not started'
            stop_reason_secondary = $null
            stop_threshold_secondary = $null
            stop_observed_secondary  = $null
            stop_sample           = $null
            termination_requested = $false
            termination_confirmed = $true
            libtest               = $null
            baseline_system       = $null
            peak_process          = $null
            system_extremes       = $null
            sample_count          = 0
            sample_interval_ms    = $SampleIntervalMs
            slowest_sample_s      = $null
            capture               = $null
            writer_dispose_error  = $null
            stdout_file           = $null
            stderr_file           = $null
            samples_file          = $null
        })))
        $sequenceAborted = $true
        foreach ($u in $Tests) { $skipped.Add($u) }
        Complete-Probe
    }

    $discoveryRefusal = Test-LaunchEnvelope -Reading $discoveryReading -Stage 'discovery_pre_launch'
    if ($null -ne $discoveryRefusal) {
        Write-Host "::warning::diagnostic limit before the discovery child for '$t': $($discoveryRefusal.Reason) (threshold $($discoveryRefusal.Threshold), observed $($discoveryRefusal.Observed))"
        Write-Note "  discovery child for '$t' not launched"
        $results.Add((New-RunRecord -Fields ([ordered]@{
            index                 = 0
            test                  = "(test discovery: $t)"
            command               = ('"{0}" {1} --exact --list' -f $exePath, $t)
            executable            = $exePath
            arguments             = @($t, '--exact', '--list')
            launched              = $false
            # A resource refusal, not a harness defect: the harness worked correctly
            # and declined to start a child on an unsafe runner.
            unlaunched_class      = 'pre-launch-refusal'
            pid                   = $null
            started_utc           = $null
            ended_utc             = $null
            elapsed_seconds       = $null
            exit_code             = $null
            outcome               = 'diagnostic-limit'
            stopped_by_watchdog   = $false
            stop_reason           = $discoveryRefusal.Reason
            stop_class            = 'diagnostic-limit'
            stop_threshold        = $discoveryRefusal.Threshold
            stop_observed         = $discoveryRefusal.Observed
            stop_detail           = "the runner was outside the safe envelope immediately before the --list discovery child for '$t'; nothing was started"
            stop_reason_secondary = $null
            stop_threshold_secondary = $null
            stop_observed_secondary  = $null
            stop_sample           = $null
            termination_requested = $false
            termination_confirmed = $true
            libtest               = $null
            baseline_system       = [pscustomobject]@{
                available_bytes     = $discoveryReading.AvailableBytes
                committed_bytes     = $discoveryReading.CommittedBytes
                commit_limit_bytes  = $discoveryReading.CommitLimitBytes
                commit_percent      = $discoveryReading.CommitPercent
                commit_percent_raw  = $discoveryReading.CommitPercentRaw
            }
            peak_process          = $null
            system_extremes       = $null
            sample_count          = 0
            sample_interval_ms    = $SampleIntervalMs
            slowest_sample_s      = $null
            capture               = $null
            writer_dispose_error  = $null
            stdout_file           = $null
            stderr_file           = $null
            samples_file          = $null
        })))
        # This child never ran, so THIS test's selection is unverified. An unverified
        # selection cannot be launched, and a run that measured nothing must not
        # report green: abort and exit 2.
        $harnessFailures.Add("the discovery child for '$t' was refused by the pre-launch safety gate; its selection is unverified and no test ran")
        $sequenceAborted = $true
        foreach ($u in $Tests) { $skipped.Add($u) }
        Complete-Probe
    }

    # ---- Gate passed for THIS child; launch it. -------------------------------
    $listed = @()
    try {
        $listed = @(& $exePath $t '--exact' '--list' 2>&1)
    } catch {
        $selectionErrors.Add("could not list '$t': $($_.Exception.Message)")
        continue
    }
    $matched = @($listed | Where-Object { $_ -match ('^' + [regex]::Escape($t) + ': test\s*$') })
    if ($matched.Count -ne 1) {
        $selectionErrors.Add("'--exact $t' names $($matched.Count) tests, not exactly 1")
    }
}

if ($selectionErrors.Count -gt 0) {
    Write-Host '::error::test selection did not resolve; refusing to run'
    foreach ($e in $selectionErrors) { Write-Host "::error::$e" }
    foreach ($e in $selectionErrors) { $harnessFailures.Add("selection: $e") }
    $sequenceAborted = $true
    foreach ($t in $Tests) { $skipped.Add($t) }
    Complete-Probe
}

# ---------------------------------------------------------------------------
# Pre-launch validation. Everything that can fail cheaply fails HERE, before the
# expensive test runs. Order: summary self-check, capture tooling, symbols. Any
# failure is a harness failure and no test is launched.
# ---------------------------------------------------------------------------

# 1. The summary path -- the failure that cost run 34505038713 its evidence.
$scratchSummary = Join-Path $outDir 'selfcheck-summary.json'
$selfCheckError = Invoke-ProbeSelfCheck -ScratchPath $scratchSummary
if ($null -ne $selfCheckError) {
    Write-Host '::error::summary self-check FAILED; refusing to launch the test'
    Write-Host "::error::$selfCheckError"
    # Deliberately NOT Complete-Probe: it uses the path just proven broken.
    Write-FallbackSummary -Path (Join-Path $outDir 'summary.json') `
                          -Reason 'summary_selfcheck_failed' -Detail $selfCheckError
    Write-FallbackSummary -Path (Join-Path $outDir 'selfcheck-failure.json') `
                          -Reason 'summary_selfcheck_failed' -Detail $selfCheckError
    exit 2
}
Remove-Item -LiteralPath $scratchSummary -Force -ErrorAction SilentlyContinue
Write-Note 'summary self-check passed (records, summary, job-summary lines, JSON write)'

# 2. Capture tooling. The helper compiles its P/Invoke surface and reports the three
#    preinstalled DLLs it binds. Nothing is downloaded and no process is opened.
$captureFailures = New-Object System.Collections.Generic.List[string]

if (-not (Test-Path -LiteralPath $CaptureHelperPath -PathType Leaf)) {
    $captureFailures.Add("capture helper not found at '$CaptureHelperPath'")
} else {
    $captureHelperFull = (Resolve-Path -LiteralPath $CaptureHelperPath).ProviderPath
    try { $pwshPath = (Get-Process -Id $PID).Path } catch { $pwshPath = $null }
    if (-not $pwshPath) { $pwshPath = Join-Path $PSHOME 'pwsh.exe' }
    if (-not (Test-Path -LiteralPath $pwshPath -PathType Leaf)) {
        $captureFailures.Add("could not resolve the PowerShell host executable for the capture helper (tried '$pwshPath')")
    } else {
        $verifyDir = Join-Path $outDir 'capture-verify'
        $null = New-Item -ItemType Directory -Force -Path $verifyDir
        $verifyResult = Join-Path $verifyDir 'capture-verify.json'
        try {
            $vp = Start-Process -FilePath $pwshPath -NoNewWindow -PassThru -ArgumentList @(
                    '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                    '-File', $captureHelperFull, '-VerifyOnly',
                    '-OutputDirectory', $verifyDir, '-ResultPath', $verifyResult) `
                -RedirectStandardOutput (Join-Path $verifyDir 'verify.stdout.txt') `
                -RedirectStandardError  (Join-Path $verifyDir 'verify.stderr.txt')
            $null = $vp.WaitForExit(60000)
            $vpExit = $null
            try { $vpExit = $vp.ExitCode } catch { $vpExit = $null }
            if ($vpExit -ne 0) {
                $captureFailures.Add("capture helper -VerifyOnly exited $vpExit")
            } elseif (-not (Test-Path -LiteralPath $verifyResult -PathType Leaf)) {
                $captureFailures.Add('capture helper -VerifyOnly wrote no result file')
            } else {
                $verify = Get-Content -LiteralPath $verifyResult -Raw | ConvertFrom-Json
                if (-not $verify.completed) {
                    $captureFailures.Add("capture tooling unavailable: $($verify.failure_reason) $($verify.failure_detail)")
                } else {
                    $captureTools = @($verify.tools)
                    foreach ($t in $captureTools) {
                        Write-Note "capture tool      : $($t.name) $($t.version) at $($t.path)"
                    }
                }
            }
        } catch {
            $captureFailures.Add("capture helper verification raised: $($_.Exception.Message)")
        }
    }
}

# 3. Symbols. A stack artifact nobody can symbolize is not worth spending the run on,
#    so a missing, ambiguous or oversized PDB fails BEFORE the test launches.
if ($captureFailures.Count -eq 0) {
    $exeItem = Get-Item -LiteralPath $exePath
    $pdbName = $exeItem.BaseName + '.pdb'
    $pdbMatches = @(Get-ChildItem -LiteralPath $exeItem.DirectoryName -Filter $pdbName -File -ErrorAction SilentlyContinue)
    if ($pdbMatches.Count -eq 0) {
        $captureFailures.Add("no matching symbols: '$pdbName' is not beside the test binary")
    } elseif ($pdbMatches.Count -ne 1) {
        $captureFailures.Add("ambiguous symbols: $($pdbMatches.Count) files match '$pdbName'")
    } elseif ($pdbMatches[0].Length -gt $captureMaxFileBytes) {
        $captureFailures.Add("symbols oversized: $($pdbMatches[0].Length) B exceeds the $captureMaxFileBytes B file cap")
    } else {
        $pdbSha = $null
        try { $pdbSha = (Get-FileHash -LiteralPath $pdbMatches[0].FullName -Algorithm SHA256).Hash.ToLowerInvariant() } catch { }
        $captureSymbol = [pscustomobject]@{
            path                  = $pdbMatches[0].FullName
            name                  = $pdbMatches[0].Name
            bytes                 = $pdbMatches[0].Length
            sha256                = $pdbSha
            matched_by            = 'exact <executable basename>.pdb adjacency'
            linkage_authoritative = $false
            linkage_note          = 'authoritative dump-to-PDB linkage is the CodeView signature/age record inside the minidump module list; verify offline against this PDB'
        }
        Write-Note "capture symbols   : $($captureSymbol.name) ($($captureSymbol.bytes) B, sha256 $($captureSymbol.sha256))"
    }
}

if ($captureFailures.Count -gt 0) {
    Write-Host '::error::capture readiness FAILED; refusing to launch the test'
    foreach ($e in $captureFailures) {
        Write-Host "::error::$e"
        $harnessFailures.Add("capture readiness: $e")
    }
    $sequenceAborted = $true
    foreach ($t in $Tests) { $skipped.Add($t) }
    Complete-Probe
}

Write-Note "binary            : $exePath"
Write-Note "output            : $outDir"
Write-Note "sample interval   : $SampleIntervalMs ms"
Write-Note "per-test timeout  : $PerTestTimeoutSeconds s (checked at the top of each iteration)"
Write-Note "cim call bound    : $CimTimeoutSec s"
Write-Note "private bytes cap : $PrivateBytesLimitGiB GiB"
Write-Note "available floor   : $AvailableFloorGiB GiB"
Write-Note "commit ceiling    : $CommitCeilingPercent %"
Write-Note "capture trigger   : $CaptureTriggerGiB GiB private bytes (below the $PrivateBytesLimitGiB GiB kill)"
Write-Note "capture bounds    : $CaptureTimeoutSeconds s, $CaptureMaxFileMiB MiB/file, $CaptureMaxTotalMiB MiB total"

foreach ($test in $Tests) {
    $index++

    if ($sequenceAborted) {
        $skipped.Add($test)
        Write-Note "skipping '$test': the sequence was aborted"
        continue
    }

    $slug = ($test -replace '[^A-Za-z0-9_.-]', '_')
    $stdoutPath  = Join-Path $outDir "$index-$slug.stdout.txt"
    $stderrPath  = Join-Path $outDir "$index-$slug.stderr.txt"
    $samplesPath = Join-Path $outDir "$index-$slug.samples.csv"

    # Default libtest capture is deliberately left ON: the failing CI job ran under
    # capture, and capture itself buffers test output in memory. Turning it off here
    # would change the very thing being measured.
    $testArgs = @($test, '--exact', '--test-threads=1')
    $commandLine = ('"{0}" {1}' -f $exePath, ($testArgs -join ' '))
    Write-Note "run $index/$($Tests.Count): $commandLine"

    $baseline = $null
    $baselineError = $null
    try {
        $baseline = Read-SystemMemory
    } catch {
        $baselineError = $_.Exception.Message
    }

    if ($null -eq $baseline) {
        # Controlled handling, not an unwind: record the failure, refuse to launch
        # anything further, and fall through to Complete-Probe so summary.json is
        # still written and the exit code is the harness-failure 2.
        $msg = "system memory telemetry became unusable before '$test': $baselineError"
        Write-Host "::error::$msg"
        Write-Note '  not launched'
        $harnessFailures.Add($msg)
        $sequenceAborted = $true
        $results.Add((New-RunRecord -Fields ([ordered]@{
            index                 = $index
            test                  = $test
            command               = $commandLine
            executable            = $exePath
            arguments             = $testArgs
            launched              = $false
            # A defect in the run, NOT a fact about the runner: the harness could not
            # read the counters it needs, so it never got as far as a safety verdict.
            unlaunched_class      = 'harness-failure'
            pid                   = $null
            started_utc           = $null
            ended_utc             = $null
            elapsed_seconds       = $null
            exit_code             = $null
            outcome               = 'harness-failure'
            stopped_by_watchdog   = $false
            stop_reason           = 'baseline_system_telemetry_lost'
            stop_class            = 'harness-failure'
            stop_threshold        = 'system memory telemetry must be readable before every launch'
            stop_observed         = $baselineError
            stop_detail           = 'no safety verdict could be reached, so no child was started'
            stop_reason_secondary = $null
            stop_threshold_secondary = $null
            stop_observed_secondary  = $null
            stop_sample           = $null
            termination_requested = $false
            termination_confirmed = $true
            libtest               = $null
            baseline_system       = $null
            peak_process          = $null
            system_extremes       = $null
            sample_count          = 0
            sample_interval_ms    = $SampleIntervalMs
            slowest_sample_s      = $null
            capture               = $null
            writer_dispose_error  = $null
            stdout_file           = $null
            stderr_file           = $null
            samples_file          = $null
        })))
        continue
    }

    Write-Note ("  baseline available {0:N0} B | committed {1:N0} B | limit {2:N0} B ({3}% raw)" -f `
        $baseline.AvailableBytes, $baseline.CommittedBytes, $baseline.CommitLimitBytes, $baseline.CommitPercentRaw)

    # ---- Pre-launch gate. -----------------------------------------------------
    # The same two system bounds the watchdog enforces DURING a run are enforced
    # BEFORE it starts. A runner already below the floor or above the ceiling is
    # not a safe place to start a memory-hungry child: the child could push the
    # machine over before the first sample is ever taken. Refusing to launch is a
    # diagnostic limit -- a fact about the runner -- not a test result.
    $refusal = Test-LaunchEnvelope -Reading $baseline -Stage 'pre_launch'
    $preLaunchReason = $null
    $preLaunchThreshold = $null
    $preLaunchObserved = $null
    if ($null -ne $refusal) {
        $preLaunchReason    = $refusal.Reason
        $preLaunchThreshold = $refusal.Threshold
        $preLaunchObserved  = $refusal.Observed
    }

    if ($null -ne $preLaunchReason) {
        Write-Host "::warning::diagnostic limit for '$test' BEFORE launch: $preLaunchReason (threshold $preLaunchThreshold, observed $preLaunchObserved)"
        Write-Note "  not launched"
        $results.Add((New-RunRecord -Fields ([ordered]@{
            index                 = $index
            test                  = $test
            command               = $commandLine
            executable            = $exePath
            arguments             = $testArgs
            launched              = $false
            unlaunched_class      = 'pre-launch-refusal'
            pid                   = $null
            started_utc           = $null
            ended_utc             = $null
            elapsed_seconds       = $null
            exit_code             = $null
            outcome               = 'diagnostic-limit'
            # Nothing was started, so nothing was terminated. Neither field may
            # imply a child that never existed.
            stopped_by_watchdog   = $false
            stop_reason           = $preLaunchReason
            stop_class            = 'diagnostic-limit'
            stop_threshold        = $preLaunchThreshold
            stop_observed         = $preLaunchObserved
            stop_detail           = 'the runner was already outside the safe envelope; no child was started'
            stop_reason_secondary = $null
            stop_threshold_secondary = $null
            stop_observed_secondary  = $null
            stop_sample           = $null
            termination_requested = $false
            termination_confirmed = $true
            libtest               = $null
            baseline_system       = [pscustomobject]@{
                available_bytes    = $baseline.AvailableBytes
                committed_bytes    = $baseline.CommittedBytes
                commit_limit_bytes = $baseline.CommitLimitBytes
                commit_percent     = $baseline.CommitPercent
                commit_percent_raw = $baseline.CommitPercentRaw
            }
            peak_process          = $null
            system_extremes       = $null
            sample_count          = 0
            sample_interval_ms    = $SampleIntervalMs
            slowest_sample_s      = $null
            capture               = $null
            writer_dispose_error  = $null
            stdout_file           = $null
            stderr_file           = $null
            samples_file          = $null
        })))
        continue
    }

    # Declared before the try so the finally block can always see them, whether or
    # not the launch itself got that far.
    $writer = $null
    $proc = $null
    $procId = 0
    $stopwatch = $null
    $startedUtc = (Get-Date).ToUniversalTime()
    $endedUtc = $null
    $fatalError = $null

    $sampleCount = 0
    $peakPrivate = [uint64]0
    $peakWorkingSet = [uint64]0
    $peakVirtual = [uint64]0
    $peakThreads = 0
    $peakHandles = 0
    $peakCommitted = [uint64]0
    $peakCommitPct = 0.0
    $minAvailable = [uint64]::MaxValue
    $stopReason = $null
    $stopThreshold = $null
    $stopObserved = $null
    $stopSample = $null
    $stopDetail = $null
    $terminationRequested = $false
    $terminationConfirmed = $false
    $maxSampleSeconds = 0.0
    $writerDisposeError = $null
    $captureState = $null

    # Everything from here to the finally can throw: the CSV writer can fail on a
    # full or read-only disk, Start-Process can fail, a counter read can surprise
    # us. None of that may leave a live child behind, so termination and exit
    # confirmation live in a finally and do not depend on any telemetry file
    # succeeding -- or even existing.
    try {
        $writer = New-Object System.IO.StreamWriter($samplesPath, $false, [System.Text.UTF8Encoding]::new($false))
        $writer.AutoFlush = $true
        $writer.WriteLine('sample,timestamp_utc,elapsed_s,pid,private_bytes,working_set_bytes,virtual_bytes,thread_count,handle_count,sys_available_bytes,sys_committed_bytes,sys_commit_limit_bytes,sys_commit_pct')

        $startedUtc = (Get-Date).ToUniversalTime()
        $proc = Start-Process -FilePath $exePath `
                              -ArgumentList $testArgs `
                              -NoNewWindow `
                              -PassThru `
                              -RedirectStandardOutput $stdoutPath `
                              -RedirectStandardError $stderrPath

        # Captured while the handle is open. The Process object holds that handle for
        # its lifetime, so the kernel cannot recycle this PID and taskkill cannot
        # reach an unrelated process.
        $procId = $proc.Id

        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

        while ($true) {
            # (1) Time first, before any counter read. A slow or hung sample cannot push
            #     the per-test timeout out indefinitely, because the next iteration tests
            #     elapsed time before it does any work at all.
            $elapsed = [math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
            if ($stopwatch.Elapsed.TotalSeconds -gt $PerTestTimeoutSeconds) {
                $stopReason    = 'per_test_timeout'
                $stopThreshold = "$PerTestTimeoutSeconds s wall clock"
                $stopObserved  = "$elapsed s"
                $stopSample    = $sampleCount
                break
            }

            # (2) Liveness.
            $exited = $true
            try { $exited = $proc.HasExited } catch { $exited = $true }
            if ($exited) { break }

            $sampleStart = $stopwatch.Elapsed.TotalSeconds

            # (3) Process counters. A failure here is only benign if the child's exit can
            #     be POSITIVELY confirmed. Otherwise the harness has a live child it can
            #     no longer measure, which is a harness failure, not a completed run.
            $priv = $null; $ws = $null; $vm = $null; $threads = $null; $handles = $null
            $procReadError = $null
            try {
                $proc.Refresh()
                $priv    = [uint64]$proc.PrivateMemorySize64
                $ws      = [uint64]$proc.WorkingSet64
                $vm      = [uint64]$proc.VirtualMemorySize64
                $threads = $proc.Threads.Count
                $handles = $proc.HandleCount
            } catch {
                $procReadError = $_.Exception.Message
            }

            if ($null -ne $procReadError) {
                $confirmedExit = $false
                try { $confirmedExit = $proc.HasExited } catch { $confirmedExit = $false }
                if ($confirmedExit) {
                    # Ordinary race: the child exited between the liveness check and the
                    # read. Not a sample, and not an error.
                    break
                }
                $stopReason    = 'process_counters_unreadable'
                $stopThreshold = 'per-process counters must remain readable while the child runs'
                $stopObserved  = $procReadError
                $stopSample    = $sampleCount
                $stopDetail    = 'the child was still running and could no longer be measured'
                break
            }

            # (4) System counters. FAIL CLOSED on the first lost or unusable sample --
            #     no grace period, because a grace period is precisely a window in which
            #     two of the four bounds are not enforced.
            $sys = $null
            $sysError = $null
            try {
                $sys = Read-SystemMemory
            } catch {
                $sysError = $_.Exception.Message
            }

            if ($null -eq $sys) {
                $stopReason    = 'system_telemetry_lost'
                $stopThreshold = 'system memory telemetry must remain readable for every sample'
                $stopObserved  = $sysError
                $stopSample    = $sampleCount
                $stopDetail    = 'the available-memory floor and commit ceiling could no longer be enforced'
                break
            }

            $sampleSeconds = $stopwatch.Elapsed.TotalSeconds - $sampleStart
            if ($sampleSeconds -gt $maxSampleSeconds) { $maxSampleSeconds = $sampleSeconds }

            # (5) Record.
            $sampleCount++
            if ($priv -gt $peakPrivate)    { $peakPrivate = $priv }
            if ($ws -gt $peakWorkingSet)   { $peakWorkingSet = $ws }
            if ($vm -gt $peakVirtual)      { $peakVirtual = $vm }
            if ($threads -gt $peakThreads) { $peakThreads = $threads }
            if ($handles -gt $peakHandles) { $peakHandles = $handles }
            if ($sys.AvailableBytes -lt $minAvailable)  { $minAvailable = $sys.AvailableBytes }
            if ($sys.CommittedBytes -gt $peakCommitted) { $peakCommitted = $sys.CommittedBytes }
            if ($sys.CommitPercentRaw -gt $peakCommitPct)  { $peakCommitPct = $sys.CommitPercentRaw }

            # $elapsed was read at the top of the iteration, before the counters. The row
            # carries the time the sample actually completed.
            $rowElapsed = [math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
            $writer.WriteLine(('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}' -f `
                $sampleCount,
                $startedUtc.AddSeconds($rowElapsed).ToString('o'),
                $rowElapsed, $procId, $priv, $ws, $vm, $threads, $handles,
                $sys.AvailableBytes, $sys.CommittedBytes, $sys.CommitLimitBytes, $sys.CommitPercent))

            # (5b) Capture trigger, checked BEFORE the kill thresholds because it
            #      sits strictly below them. The helper suspends the target before
            #      writing anything, so growth stops here rather than racing the
            #      capture; the private-bytes kill below stays the fail-safe.
            if ($null -eq $captureState -and $priv -gt $captureTriggerBytes) {
                Write-Note ("  capture trigger reached at {0:N0} B private; suspending and capturing PID {1}" -f $priv, $procId)
                $captureState = Invoke-BoundedCapture -ProcessId $procId -PrivateBytes $priv `
                                                      -CaptureDirectory (Join-Path $outDir "capture-$index")
                $stopSample = $sampleCount
                if ($captureState.completed) {
                    $stopReason    = 'process_private_bytes_over_capture_trigger'
                    $stopThreshold = "$captureTriggerBytes bytes ($CaptureTriggerGiB GiB private bytes, capture trigger)"
                    $stopObserved  = "$priv bytes private"
                    $stopDetail    = "stacks and region evidence captured in $($captureState.elapsed_s) s across $($captureState.total_bytes) B; the target was suspended and is terminated next"
                    Write-Note ("  capture completed in {0} s, {1:N0} B across {2} files" -f `
                        $captureState.elapsed_s, $captureState.total_bytes, @($captureState.files).Count)
                } else {
                    # No retry, by design. Partial evidence stays on disk, and the
                    # existing cleanup path terminates the tree next regardless.
                    $stopReason    = 'capture_failed'
                    $stopThreshold = "capture must complete within $CaptureTimeoutSeconds s, $CaptureMaxFileMiB MiB/file and $CaptureMaxTotalMiB MiB total"
                    $stopObserved  = "$($captureState.failure_reason): $($captureState.failure_detail)"
                    $stopDetail    = 'capture failed; partial evidence preserved, not retried'
                    Write-Host "::warning::capture failed for '$test': $($captureState.failure_reason)"
                }
                break
            }

            # (6) Memory bounds. Every one of these has a usable reading behind it by the
            #     time control reaches here.
            if ($priv -gt $privateLimitBytes) {
                $stopReason    = 'process_private_bytes_over_limit'
                $stopThreshold = "$privateLimitBytes bytes ($PrivateBytesLimitGiB GiB private bytes)"
                $stopObserved  = "$priv bytes private"
            } elseif ($sys.AvailableBytes -lt $availableFloorBytes) {
                $stopReason    = 'system_available_memory_below_floor'
                $stopThreshold = "$availableFloorBytes bytes ($AvailableFloorGiB GiB available physical)"
                $stopObserved  = "$($sys.AvailableBytes) bytes available"
            } elseif ($sys.CommitPercentRaw -gt $CommitCeilingPercent) {
                $stopReason    = 'system_commit_over_ceiling'
                $stopThreshold = "$CommitCeilingPercent% of a $($sys.CommitLimitBytes)-byte commit limit"
                $stopObserved  = "$($sys.CommitPercentRaw)% raw ($($sys.CommittedBytes) bytes committed)"
            }

            if ($null -ne $stopReason) {
                $stopSample = $sampleCount
                break
            }

            Start-Sleep -Milliseconds $SampleIntervalMs
        }
    }
    catch {
        # Recorded, not swallowed: the finally still runs, then this is classified
        # as a harness failure and the sequence aborts.
        $fatalError = $_.Exception.Message
    }
    finally {
        # ---- Unconditional cleanup. --------------------------------------------
        # Reached on every path out of the try: normal completion, `break`, a CSV
        # write failure, a failed launch, or any other terminating error. Child
        # termination and exit confirmation must never depend on telemetry-file
        # success, so the writer is disposed defensively and FIRST, and a failure
        # to dispose it cannot skip the kill below.
        if ($null -ne $writer) {
            try {
                $writer.Dispose()
            } catch {
                # Swallowed HERE only so it cannot skip the termination below. It is
                # classified immediately after the finally: a CSV that failed to
                # flush and close is incomplete evidence and must not be reported as
                # an ordinary completed run.
                $writerDisposeError = $_.Exception.Message
                Write-Note "  csv writer dispose failed: $writerDisposeError"
            }
        }
        if ($null -ne $stopwatch) { $stopwatch.Stop() }
        $endedUtc = (Get-Date).ToUniversalTime()

        if ($null -eq $proc) {
            # The launch itself never produced a process, so there is no child to
            # confirm. Nothing was left running.
            $terminationConfirmed = $true
        } else {
            $exitedCleanly = $false
            try { $exitedCleanly = $proc.HasExited } catch { $exitedCleanly = $false }
            if ($exitedCleanly) {
                $terminationConfirmed = $true
            } else {
                $terminationRequested = $true
                $terminationConfirmed = Stop-TestTree -Process $proc -ProcessId $procId -WaitMilliseconds $TerminationWaitMs
            }
        }
    }

    # ---- Classification. -------------------------------------------------------
    # Termination and exit confirmation already happened, unconditionally, in the
    # finally above. Nothing here re-decides whether the child is gone; it only
    # reports what that block established.
    if ($null -ne $fatalError -and $null -eq $stopReason) {
        $stopReason    = 'harness_exception'
        $stopThreshold = 'the sampling loop must not throw'
        $stopObserved  = $fatalError
        $stopDetail    = 'the child was terminated by the unconditional cleanup path'
    }

    # What the run stopped for BEFORE a dispose failure can override it. Captured so
    # that overriding preserves the earlier fact instead of erasing it.
    $primaryStopReason = $stopReason
    $watchdogFired = ($null -ne $primaryStopReason) -and ($diagnosticLimitReasons -contains $primaryStopReason)
    $stopReasonSecondary = $null
    $stopThresholdSecondary = $null
    $stopObservedSecondary = $null

    # A dispose failure is classified WHATEVER else already stopped the run --
    # including a diagnostic limit. A samples file that would not flush and close may
    # be truncated, so this run's telemetry cannot be trusted regardless of why the
    # child stopped, and "the watchdog fired" must not stand in for "the evidence is
    # intact". The earlier reason is kept as secondary evidence; it does not mask
    # this one.
    if ($null -ne $writerDisposeError) {
        if ($null -ne $stopReason -and $stopReason -ne 'csv_writer_dispose_failed') {
            $stopReasonSecondary    = $stopReason
            $stopThresholdSecondary = $stopThreshold
            $stopObservedSecondary  = $stopObserved
        }
        $stopReason    = 'csv_writer_dispose_failed'
        $stopThreshold = 'the samples file must flush and close cleanly'
        $stopObserved  = $writerDisposeError
        $stopDetail    = 'the CSV evidence for this run is incomplete and cannot be read as a complete sample series'
        if ($null -ne $stopReasonSecondary) {
            $stopDetail = "$stopDetail; the run had already stopped for '$stopReasonSecondary', preserved as secondary evidence"
        }
    }

    # An unconfirmed termination outranks everything else, dispose failure included:
    # a child that may still be running invalidates this run and every later one, so
    # it becomes the EFFECTIVE reason rather than only flipping the class. Leaving a
    # diagnostic reason effective while the class says harness failure is exactly the
    # disagreement this override exists to prevent, and a null reason under a
    # harness-failure class is no better.
    if (-not $terminationConfirmed) {
        # Demote the current effective reason ONLY if the secondary triplet is free.
        # In the dispose-over-bound case it already holds the bound, and that bound
        # must not be overwritten; the dispose failure stays visible through
        # `writer_dispose_error` and its own aggregate entry below.
        if ($null -eq $stopReasonSecondary -and $null -ne $stopReason) {
            $stopReasonSecondary    = $stopReason
            $stopThresholdSecondary = $stopThreshold
            $stopObservedSecondary  = $stopObserved
        }
        $stopReason    = 'termination_unconfirmed'
        $stopThreshold = 'the captured child tree must be confirmed gone before the run is classified'
        $stopObserved  = "PID $procId could not be confirmed terminated within $TerminationWaitMs ms"
        $stopDetail    = 'a child that may still be running invalidates this run and every later one; the sequence aborts'
        if ($null -ne $writerDisposeError) {
            $stopDetail = "$stopDetail; the samples file also failed to close, so this run's CSV evidence is incomplete"
        }
        if ($null -ne $stopReasonSecondary) {
            $stopDetail = "$stopDetail; the run had already stopped for '$stopReasonSecondary', preserved as secondary evidence"
        }
    }

    $isDiagnosticLimit = ($null -ne $stopReason) -and ($diagnosticLimitReasons -contains $stopReason)

    if ($null -ne $stopReason) {
        $label = if ($isDiagnosticLimit) { 'diagnostic limit' } else { 'HARNESS FAILURE' }
        Write-Host "::warning::$label for '$test': $stopReason (threshold $stopThreshold, observed $stopObserved)"
    }

    if ($null -ne $fatalError -or $null -ne $writerDisposeError) {
        # An exception mid-run, or a samples file that would not close, means this
        # test's telemetry is incomplete. The sequence stops rather than pretending
        # the next run is comparable.
        $sequenceAborted = $true
    }

    if (-not $terminationConfirmed) {
        # A requested kill that cannot be confirmed leaves a live child. Launching the
        # next test beside it would destroy the isolation the whole experiment rests on.
        $msg = "could not confirm termination of PID $procId for '$test'; aborting the sequence rather than running the next test beside a live child"
        Write-Host "::error::$msg"
        $harnessFailures.Add($msg)
        $sequenceAborted = $true
    }

    # Dedicated aggregate entries for the two facts that can be demoted out of the
    # effective reason. Without these, a dispose failure or a sampling exception that
    # lost primacy to `termination_unconfirmed` would survive only in a structured
    # field and vanish from harness_failures.
    if ($null -ne $writerDisposeError) {
        $msg = "'$test' could not flush and close its samples file ($writerDisposeError); its CSV evidence is incomplete"
        Write-Host "::error::$msg"
        $harnessFailures.Add($msg)
    }
    if ($null -ne $fatalError) {
        $msg = "'$test' raised during sampling ($fatalError); the child was terminated by the unconditional cleanup path"
        Write-Host "::error::$msg"
        $harnessFailures.Add($msg)
    }

    # Everything else that classifies as a harness failure. The three reasons listed
    # already have their own entry above (or, for termination_unconfirmed, from the
    # block that detected it), so this does not restate them.
    if ($null -ne $stopReason -and -not $isDiagnosticLimit -and
        $stopReason -notin @('csv_writer_dispose_failed', 'harness_exception', 'termination_unconfirmed')) {
        $detail = if ($stopDetail) { " -- $stopDetail" } else { '' }
        $msg = "'$test' stopped by $stopReason ($stopObserved)$detail"
        Write-Host "::error::$msg"
        $harnessFailures.Add($msg)
    }

    $exitCode = $null
    if ($null -ne $proc) {
        try { $exitCode = $proc.ExitCode } catch { $exitCode = $null }
    }

    # The stopwatch never started if Start-Process threw.
    $elapsedSeconds = $null
    if ($null -ne $stopwatch) { $elapsedSeconds = [math]::Round($stopwatch.Elapsed.TotalSeconds, 3) }

    $stdoutText = if (Test-Path -LiteralPath $stdoutPath) { Get-Content -LiteralPath $stdoutPath -Raw -ErrorAction SilentlyContinue } else { '' }
    $stderrText = if (Test-Path -LiteralPath $stderrPath) { Get-Content -LiteralPath $stderrPath -Raw -ErrorAction SilentlyContinue } else { '' }
    if ($null -eq $stdoutText) { $stdoutText = '' }
    if ($null -eq $stderrText) { $stderrText = '' }
    $combined = "$stdoutText`n$stderrText"

    $running = $null
    $m = [regex]::Match($combined, '(?m)^running (\d+) tests?\r?$')
    if ($m.Success) { $running = [int]$m.Groups[1].Value }

    $resultLine = $null
    $passed = $null; $failed = $null; $ignored = $null
    $r = [regex]::Match($combined, '(?m)^test result: (?<status>\w+)\. (?<passed>\d+) passed; (?<failed>\d+) failed; (?<ignored>\d+) ignored')
    if ($r.Success) {
        $resultLine = $r.Value
        $passed  = [int]$r.Groups['passed'].Value
        $failed  = [int]$r.Groups['failed'].Value
        $ignored = [int]$r.Groups['ignored'].Value
    }

    $zeroSelected = ($null -ne $running -and $running -eq 0)
    if ($zeroSelected) {
        $msg = "selection '--exact $test' matched ZERO tests; the run proves nothing"
        Write-Host "::error::$msg"
        $harnessFailures.Add($msg)
    } elseif ($null -eq $running -and $null -eq $stopReason) {
        $msg = "no libtest 'running N tests' line for '$test'; the binary produced no recognisable selection output"
        Write-Host "::error::$msg"
        $harnessFailures.Add($msg)
    }

    $outcome = 'process-completed'
    if ($null -ne $stopReason) {
        $outcome = if ($isDiagnosticLimit) { 'diagnostic-limit' } else { 'harness-failure' }
    }

    $minAvailableOut = $null
    if ($minAvailable -ne [uint64]::MaxValue) { $minAvailableOut = $minAvailable }

    $stopClass = $null
    if ($null -ne $stopReason) {
        $stopClass = if ($isDiagnosticLimit) { 'diagnostic-limit' } else { 'harness-failure' }
    }
    # No separate forcing for an unconfirmed termination: it is already the effective
    # `termination_unconfirmed` stop_reason, which is not a diagnostic limit, so the
    # class follows from the reason. One source of truth, not three that can drift.

    $endedUtcOut = $null
    if ($null -ne $endedUtc) { $endedUtcOut = $endedUtc.ToString('o') }

    # Only meaningful when the child never started. A launched run is neither kind.
    $unlaunchedClass = $null
    if ($null -eq $proc) { $unlaunchedClass = 'harness-failure' }

    Write-Note ("  outcome {0} | exit {1} | {2} s | {3} samples | slowest sample {4:N3} s" -f `
        $outcome, $exitCode, $elapsedSeconds, $sampleCount, $maxSampleSeconds)
    Write-Note ("  peak private {0:N0} B | peak working set {1:N0} B | peak virtual {2:N0} B | peak threads {3}" -f `
        $peakPrivate, $peakWorkingSet, $peakVirtual, $peakThreads)

    $results.Add((New-RunRecord -Fields ([ordered]@{
        index                 = $index
        test                  = $test
        command               = $commandLine
        executable            = $exePath
        arguments             = $testArgs
        launched              = ($null -ne $proc)
        unlaunched_class      = $unlaunchedClass
        pid                   = $procId
        started_utc           = $startedUtc.ToString('o')
        ended_utc             = $endedUtcOut
        elapsed_seconds       = $elapsedSeconds
        exit_code             = $exitCode
        outcome               = $outcome
        # `stopped_by_watchdog` is true ONLY when a diagnostic bound fired AND the
        # child is confirmed gone. A kill that was merely requested does not qualify.
        # It reads the ORIGINAL reason, so a dispose failure that reclassifies the run
        # as a harness failure does not erase the fact that a bound fired; stop_class
        # and outcome carry the harness failure independently.
        stopped_by_watchdog   = ($watchdogFired -and $terminationConfirmed)
        stop_reason           = $stopReason
        stop_reason_secondary = $stopReasonSecondary
        stop_threshold_secondary = $stopThresholdSecondary
        stop_observed_secondary  = $stopObservedSecondary
        stop_class            = $stopClass
        stop_threshold        = $stopThreshold
        stop_observed         = $stopObserved
        stop_detail           = $stopDetail
        writer_dispose_error  = $writerDisposeError
        stop_sample           = $stopSample
        termination_requested = $terminationRequested
        termination_confirmed = $terminationConfirmed
        libtest               = [pscustomobject]@{
            running_count       = $running
            result_line         = $resultLine
            passed              = $passed
            failed              = $failed
            ignored             = $ignored
            zero_tests_selected = $zeroSelected
        }
        baseline_system       = [pscustomobject]@{
            available_bytes    = $baseline.AvailableBytes
            committed_bytes    = $baseline.CommittedBytes
            commit_limit_bytes = $baseline.CommitLimitBytes
            commit_percent     = $baseline.CommitPercent
        }
        peak_process          = [pscustomobject]@{
            # Private bytes and working set are different quantities, not two views
            # of one. Observed at the sample interval, so read them as lower bounds.
            private_bytes     = $peakPrivate
            working_set_bytes = $peakWorkingSet
            virtual_bytes     = $peakVirtual
            thread_count      = $peakThreads
            handle_count      = $peakHandles
        }
        system_extremes       = [pscustomobject]@{
            min_available_bytes    = $minAvailableOut
            max_committed_bytes    = $peakCommitted
            max_commit_percent     = [math]::Round($peakCommitPct, 3)
            max_commit_percent_raw = $peakCommitPct
        }
        sample_count          = $sampleCount
        sample_interval_ms    = $SampleIntervalMs
        slowest_sample_s      = [math]::Round($maxSampleSeconds, 3)
        capture               = $captureState
        stdout_file           = Split-Path -Leaf $stdoutPath
        stderr_file           = Split-Path -Leaf $stderrPath
        samples_file          = Split-Path -Leaf $samplesPath
    })))
}

Complete-Probe
