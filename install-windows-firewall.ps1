#Requires -Version 5.1
# Allow the NAS (and LAN) to reach STL Search on this PC so the Synology
# gateway can prefer Windows for remote HTTPS / PC-folder downloads.
param(
  [string]$RuleName = "STL Search (8787)",
  [string]$LocalPort = "8787",
  [string]$RemoteAddress = "192.168.0.0/24"
)

$ErrorActionPreference = "Stop"

$existing = Get-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue
if ($existing) {
  Write-Host "Firewall rule already exists: $RuleName"
  exit 0
}

# Needs elevation
$principal = New-Object Security.Principal.WindowsPrincipal(
    [Security.Principal.WindowsIdentity]::GetCurrent()
)
$isAdmin = $principal.IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)

if (-not $isAdmin) {
  Write-Host "Re-launching elevated to add firewall rule..."
  $arg = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
  Start-Process powershell -Verb RunAs -ArgumentList $arg -Wait
  exit $LASTEXITCODE
}

New-NetFirewallRule `
  -DisplayName $RuleName `
  -Direction Inbound `
  -Action Allow `
  -Protocol TCP `
  -LocalPort $LocalPort `
  -RemoteAddress $RemoteAddress `
  -Profile Private,Domain `
  -Description "Synology STL gateway -> Windows STL Search" | Out-Null

Write-Host "Added inbound allow TCP $LocalPort from $RemoteAddress ($RuleName)"
