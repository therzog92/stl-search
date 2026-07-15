# Start or restart the STL Search web UI, then open it in the browser.
$ErrorActionPreference = "SilentlyContinue"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Port = 8787
$Url = "http://127.0.0.1:$Port"

Set-Location $Root

if (-not (Test-Path $Python)) {
    Add-Type -AssemblyName PresentationFramework
    [System.Windows.MessageBox]::Show(
        "Python virtualenv not found at:`n$Python`n`nRun once:`npython -m venv .venv`n.\.venv\Scripts\pip install -r requirements.txt",
        "STL Search",
        "OK",
        "Error"
    ) | Out-Null
    exit 1
}

# Stop anything already listening on the app port
$pids = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
foreach ($procId in $pids) {
    if ($procId -and $procId -ne 0) {
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    }
}
Start-Sleep -Seconds 1

# Ensure LAN/NAS can reach this PC (needed for Synology gateway failover).
# Non-fatal if UAC is declined.
try {
    $fw = Join-Path $Root "install-windows-firewall.ps1"
    if (Test-Path $fw) {
        $rule = Get-NetFirewallRule -DisplayName "STL Search (8787)" -ErrorAction SilentlyContinue
        if (-not $rule) {
            Start-Process powershell -Verb RunAs -ArgumentList @(
                "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $fw
            ) -Wait -ErrorAction SilentlyContinue
        }
    }
} catch {}

# Launch server in a minimized console so you can stop it later if needed.
# run.py keeps the PC from sleeping while STL Search is running (Windows).
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $Python
$psi.Arguments = "run.py"
$psi.WorkingDirectory = $Root
$psi.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Minimized
$psi.UseShellExecute = $true
[System.Diagnostics.Process]::Start($psi) | Out-Null

# Wait until the server answers, then open the browser
$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 250
    try {
        $resp = Invoke-WebRequest -Uri "$Url/health" -UseBasicParsing -TimeoutSec 1
        if ($resp.StatusCode -eq 200) {
            $ready = $true
            break
        }
    } catch {}
}

Start-Process $Url

if (-not $ready) {
    Add-Type -AssemblyName PresentationFramework
    [System.Windows.MessageBox]::Show(
        "Started STL Search, but it did not respond on $Url yet.`nCheck the minimized Python window for errors.",
        "STL Search",
        "OK",
        "Warning"
    ) | Out-Null
}
