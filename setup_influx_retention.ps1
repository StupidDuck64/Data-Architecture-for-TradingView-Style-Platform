# setup_influx_retention.ps1
# ─────────────────────────────────────────────────────────────────────────────
# Set InfluxDB bucket retention policy to 90 days for crypto bucket
# PowerShell version for Windows
# ─────────────────────────────────────────────────────────────────────────────

# Load .env file
if (Test-Path ".env") {
    Get-Content .env | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
        }
    }
}

$INFLUX_TOKEN = $env:INFLUX_TOKEN
$INFLUX_ORG = if ($env:INFLUX_ORG) { $env:INFLUX_ORG } else { "vi" }
$INFLUX_BUCKET = if ($env:INFLUX_BUCKET) { $env:INFLUX_BUCKET } else { "crypto" }
$RETENTION_SECONDS = 90 * 24 * 3600  # 90 days = 7,776,000 seconds

Write-Host "=== InfluxDB Retention Policy Setup ===" -ForegroundColor Cyan
Write-Host "Bucket: $INFLUX_BUCKET"
Write-Host "Retention: 90 days (7,776,000 seconds)"
Write-Host ""

# Wait for InfluxDB to be ready
Write-Host "Waiting for InfluxDB..." -ForegroundColor Yellow
for ($i = 1; $i -le 30; $i++) {
    try {
        docker exec influxdb influx ping | Out-Null
        Write-Host "InfluxDB is ready!" -ForegroundColor Green
        break
    }
    catch {
        Write-Host "Attempt $i/30..."
        Start-Sleep -Seconds 2
    }
}

# Update bucket retention policy
Write-Host ""
Write-Host "Setting retention policy for bucket '$INFLUX_BUCKET'..." -ForegroundColor Yellow

# Create config first (ignore if already exists)
docker exec influxdb influx config create `
    --config-name default `
    --host-url http://localhost:8086 `
    --org "$INFLUX_ORG" `
    --token "$INFLUX_TOKEN" `
    --active 2>&1 | Out-Null

# Get bucket ID by parsing bucket list output
$bucketList = docker exec influxdb influx bucket list 2>&1 | Select-String -Pattern "^\w+\s+$INFLUX_BUCKET\s+"
if ($bucketList) {
    $bucketId = ($bucketList.Line -split '\s+')[0]
    Write-Host "Found bucket ID: $bucketId"
    
    # Update retention
    docker exec influxdb influx bucket update `
        --id "$bucketId" `
        --retention "${RETENTION_SECONDS}s"
        
    Write-Host ""
    Write-Host "Retention policy set to 90 days!" -ForegroundColor Green
} else {
    Write-Host "Error: Bucket '$INFLUX_BUCKET' not found!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Verify with:" -ForegroundColor Cyan
Write-Host '  docker exec influxdb influx bucket list' -ForegroundColor Gray
