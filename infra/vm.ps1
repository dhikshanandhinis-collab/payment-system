<#
.SYNOPSIS
  Session-based operation of the production VM to save Azure credit.

.EXAMPLE
  ./infra/vm.ps1 start     # start VM, deploy latest main, wait until healthy
  ./infra/vm.ps1 status    # power state + service health
  ./infra/vm.ps1 grafana   # SSH tunnel; open http://localhost:3000 (Ctrl+C to close)
  ./infra/vm.ps1 ssh
  ./infra/vm.ps1 stop      # stop the stack cleanly, then deallocate (compute billing stops)
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory, Position = 0)]
    [ValidateSet("start", "stop", "status", "ssh", "grafana")]
    [string]$Action,
    [string]$ResourceGroup = "payment-system-prod",
    [string]$VmName = "payment-system"
)
$ErrorActionPreference = "Stop"

$keyDir = Join-Path $PSScriptRoot ".keys"
$adminKey = Join-Path $keyDir "admin_ed25519"
$knownHosts = Join-Path $keyDir "known_hosts"
$compose = "docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile observability"

function Get-PowerState {
    az vm get-instance-view -g $ResourceGroup -n $VmName --query "instanceView.statuses[?starts_with(code, 'PowerState/')].displayStatus | [0]" -o tsv
}

function Get-VmIp {
    az vm show -d -g $ResourceGroup -n $VmName --query publicIps -o tsv
}

function Invoke-Vm([string]$Command, [string[]]$Extra = @()) {
    $sshArgs = @("-i", $adminKey, "-o", "UserKnownHostsFile=$knownHosts", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10") + $Extra + @("ubuntu@$(Get-VmIp)")
    if ($Command) { $sshArgs += $Command }
    ssh @sshArgs
}

function Wait-Ssh {
    $ip = Get-VmIp
    for ($i = 0; $i -lt 30; $i++) {
        if ((Test-NetConnection $ip -Port 22 -WarningAction SilentlyContinue).TcpTestSucceeded) { return }
        Start-Sleep -Seconds 10
    }
    throw "SSH on $ip not reachable after 5 minutes."
}

az account show *> $null
if ($LASTEXITCODE -ne 0) { throw "Run 'az login' first." }

switch ($Action) {
    "start" {
        Write-Host "Starting $VmName..."
        az vm start -g $ResourceGroup -n $VmName --output none
        Wait-Ssh
        Write-Host "Deploying latest main and waiting for healthy services..."
        Invoke-Vm "cd ~/payment-system && bash deploy/deploy.sh origin/main"
        if ($LASTEXITCODE -ne 0) { throw "Deploy failed; inspect with: ./infra/vm.ps1 ssh" }
        Write-Host "Running. Remember: ./infra/vm.ps1 stop  (auto-shutdown also runs nightly)"
    }
    "stop" {
        if ((Get-PowerState) -eq "VM running") {
            Write-Host "Stopping services cleanly..."
            Invoke-Vm "cd ~/payment-system && $compose stop -t 60"
        }
        Write-Host "Deallocating $VmName (compute billing stops; disk and IP still cost a little)..."
        az vm deallocate -g $ResourceGroup -n $VmName --output none
        Write-Host "Power state: $(Get-PowerState)"
    }
    "status" {
        $state = Get-PowerState
        Write-Host "Power state: $state"
        if ($state -eq "VM running") {
            Invoke-Vm "cd ~/payment-system && git log -1 --format='commit %h %s' && $compose ps --format 'table {{.Service}}\t{{.Status}}'"
        }
    }
    "ssh" { Invoke-Vm "" }
    "grafana" {
        Write-Host "Open http://localhost:3000  (user admin; password: grep GRAFANA_ADMIN_PASSWORD ~/payment-system/.env on the VM). Ctrl+C to close."
        Invoke-Vm "" @("-N", "-L", "3000:localhost:3000", "-L", "9090:localhost:9090")
    }
}
