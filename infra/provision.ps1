<#
.SYNOPSIS
  One-command production setup: creates the Azure VM (Terraform), which installs and starts the
  stack on first boot, then configures GitHub so every push to main is tested and deployed automatically.

.EXAMPLE
  ./infra/provision.ps1                 # shows the Terraform plan and asks for confirmation
  ./infra/provision.ps1 -AutoApprove
#>
[CmdletBinding()]
param(
    [string]$Repo = "dhikshanandhinis-collab/payment-system",
    [switch]$AutoApprove
)
$ErrorActionPreference = "Stop"

$tfDir = Join-Path $PSScriptRoot "azure"
$keyDir = Join-Path $PSScriptRoot ".keys"
$adminKey = Join-Path $keyDir "admin_ed25519"
$deployKey = Join-Path $keyDir "deploy_ed25519"
$knownHosts = Join-Path $keyDir "known_hosts"

function Invoke-Native([scriptblock]$Command, [string]$What) {
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$What failed (exit $LASTEXITCODE)" }
}

# ---- Preconditions --------------------------------------------------------------------------------
foreach ($tool in "az", "terraform", "gh", "ssh", "ssh-keygen", "ssh-keyscan") {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "$tool not found. Install: winget install Microsoft.AzureCLI; winget install Hashicorp.Terraform; winget install GitHub.cli"
    }
}
$subscription = az account show --query id -o tsv 2>$null
if ($LASTEXITCODE -ne 0 -or -not $subscription) { throw "Run 'az login' first (your Azure for Students account)." }
$env:ARM_SUBSCRIPTION_ID = $subscription.Trim()
Write-Host "Azure subscription: $(az account show --query name -o tsv) ($env:ARM_SUBSCRIPTION_ID)"
gh auth status *> $null
if ($LASTEXITCODE -ne 0) { throw "Run 'gh auth login' first, signed in as the owner of $Repo." }
if (-not (Test-Path (Join-Path $tfDir "terraform.tfvars"))) {
    throw "Copy infra/azure/terraform.tfvars.example to infra/azure/terraform.tfvars and fill it in."
}

# ---- SSH keys: one for you (admin), one only for GitHub Actions (deploy) ---------------------------
New-Item -ItemType Directory -Force $keyDir | Out-Null
# Windows PowerShell 5.1 drops a bare '' argument to native commands; 7.3+ passes it correctly.
$emptyPassphrase = if ($PSVersionTable.PSVersion -ge [version]"7.3") { "" } else { '""' }
foreach ($key in $adminKey, $deployKey) {
    if (-not (Test-Path $key)) {
        Invoke-Native { ssh-keygen -q -t ed25519 -N $emptyPassphrase -C (Split-Path $key -Leaf) -f $key } "ssh-keygen"
        if ($PSVersionTable.PSEdition -eq "Desktop" -or $IsWindows) {
            icacls $key /inheritance:r /grant:r "$($env:USERNAME):F" | Out-Null
        }
    }
}

# ---- Infrastructure -------------------------------------------------------------------------------
$tfVars = @("-var=admin_ssh_public_key_path=$($adminKey -replace '\\', '/').pub",
            "-var=deploy_ssh_public_key_path=$($deployKey -replace '\\', '/').pub")
Invoke-Native { terraform "-chdir=$tfDir" init -input=false } "terraform init"
$applyArgs = @("-chdir=$tfDir", "apply", "-input=false") + $tfVars
if ($AutoApprove) { $applyArgs += "-auto-approve" }
Invoke-Native { terraform @applyArgs } "terraform apply"
$ip = (terraform "-chdir=$tfDir" output -raw public_ip).Trim()
Write-Host "VM public IP: $ip"

# ---- Wait for SSH, pin the host key ----------------------------------------------------------------
$hostKeys = $null
for ($i = 0; $i -lt 60 -and -not $hostKeys; $i++) {
    $hostKeys = ssh-keyscan -T 5 $ip 2>$null
    if (-not $hostKeys) { Start-Sleep -Seconds 10 }
}
if (-not $hostKeys) { throw "VM $ip did not open SSH within 10 minutes." }
$hostKeys | Set-Content -Encoding ascii $knownHosts

# ---- GitHub: production environment, deploy secrets, enable the deploy job ------------------------
Invoke-Native { gh api -X PUT "repos/$Repo/environments/production" --silent } "create environment"
Invoke-Native { gh secret set DEPLOY_HOST --repo $Repo --env production --body $ip } "secret DEPLOY_HOST"
Invoke-Native { gh secret set DEPLOY_USER --repo $Repo --env production --body "ubuntu" } "secret DEPLOY_USER"
Invoke-Native { Get-Content -Raw $deployKey | gh secret set DEPLOY_SSH_KEY --repo $Repo --env production } "secret DEPLOY_SSH_KEY"
Invoke-Native { Get-Content -Raw $knownHosts | gh secret set DEPLOY_KNOWN_HOSTS --repo $Repo --env production } "secret DEPLOY_KNOWN_HOSTS"
Invoke-Native { gh variable set DEPLOY_ENABLED --repo $Repo --body "true" } "variable DEPLOY_ENABLED"

# ---- Wait for first-boot install + deploy to finish ------------------------------------------------
Write-Host "Waiting for first boot (install Docker, build images, start the stack; ~10-20 min)..."
$ssh = @("-i", $adminKey, "-o", "UserKnownHostsFile=$knownHosts", "-o", "StrictHostKeyChecking=yes", "ubuntu@$ip")
ssh @ssh "cloud-init status --wait > /dev/null; cloud-init status --long; docker compose -f ~/payment-system/docker-compose.yml -f ~/payment-system/docker-compose.prod.yml --profile observability ps --format 'table {{.Service}}\t{{.Status}}'"

Write-Host ""
Write-Host "Done. Every push to main now runs CI and deploys to $ip."
Write-Host "SSH:     ssh -i $adminKey ubuntu@$ip"
Write-Host "Grafana: ssh -i $adminKey -L 3000:localhost:3000 ubuntu@$ip   then open http://localhost:3000"
Write-Host "         (admin password: grep GRAFANA_ADMIN_PASSWORD ~/payment-system/.env on the VM)"
