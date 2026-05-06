# setup_influx_retention.ps1
# ─────────────────────────────────────────────────────────────────────────────
# Set up InfluxDB retention tasks for candle data (PowerShell version for Windows)
# - 1s candles: 7 days retention (sliding window)
# - 1m candles: 90 days retention (sliding window)
# ─────────────────────────────────────────────────────────────────────────────

# Load .env file
if (Test-Path "d41d8cd9 (1).env") {
    Get-Content "d41d8cd9 (1).env" | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
        }
    }
}

$INFLUX_TOKEN = $env:INFLUX_TOKEN
$INFLUX_ORG = if ($env:INFLUX_ORG) { $env:INFLUX_ORG } else { "vi" }
$INFLUX_BUCKET = if ($env:INFLUX_BUCKET) { $env:INFLUX_BUCKET } else { "crypto" }

Write-Host "=== InfluxDB Retention Policy Setup ===" -ForegroundColor Cyan
Write-Host "Organization: $INFLUX_ORG"
Write-Host "Bucket: $INFLUX_BUCKET"
Write-Host "  - 1s candles: 7 days retention"
Write-Host "  - 1m candles: 90 days retention"
Write-Host ""

# Wait for InfluxDB to be ready
Write-Host "Waiting for InfluxDB..." -ForegroundColor Yellow
for ($i = 1; $i -le 30; $i++) {
    try {
        docker exec influxdb influx ping 2>&1 | Out-Null
        Write-Host "InfluxDB is ready!" -ForegroundColor Green
        break
    }
    catch {
        Write-Host "Attempt $i/30..."
        Start-Sleep -Seconds 2
    }
}

# Create config (ignore if already exists)
Write-Host ""
Write-Host "Configuring InfluxDB CLI..." -ForegroundColor Yellow
docker exec influxdb influx config create `
    --config-name default `
    --host-url http://localhost:8086 `
    --org "$INFLUX_ORG" `
    --token "$INFLUX_TOKEN" `
    --active 2>&1 | Out-Null

# Create task for 1s candles retention (7 days)
Write-Host ""
Write-Host "Creating retention task for 1s candles (7 days)..." -ForegroundColor Yellow

$task1s = @"
option task = {name: \"delete_old_1s_candles\", every: 6h}

from(bucket: \"$INFLUX_BUCKET\")
  |> range(start: -8d, stop: -7d)
  |> filter(fn: (r) => r[\"_measurement\"] == \"candles\")
  |> filter(fn: (r) => r[\"interval\"] == \"1s\")
  |> drop()
"@

$task1s | docker exec -i influxdb influx task create `
    --org "$INFLUX_ORG" 2>&1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Task 'delete_old_1s_candles' created" -ForegroundColor Green
} else {
    Write-Host "⚠ Task might already exist, continuing..." -ForegroundColor Yellow
}

# Create task for 1m candles retention (90 days)
Write-Host ""
Write-Host "Creating retention task for 1m candles (90 days)..." -ForegroundColor Yellow

$task1m = @"
option task = {name: \"delete_old_1m_candles\", every: 1d}

from(bucket: \"$INFLUX_BUCKET\")
  |> range(start: -95d, stop: -90d)
  |> filter(fn: (r) => r[\"_measurement\"] == \"candles\")
  |> filter(fn: (r) => r[\"interval\"] == \"1m\")
  |> drop()
"@

$task1m | docker exec -i influxdb influx task create `
    --org "$INFLUX_ORG" 2>&1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Task 'delete_old_1m_candles' created" -ForegroundColor Green
} else {
    Write-Host "⚠ Task might already exist, continuing..." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Setup Complete ===" -ForegroundColor Green
Write-Host "InfluxDB retention tasks configured successfully!"
Write-Host "  ✓ 1s candles: 7 days sliding window (cleanup every 6 hours)"
Write-Host "  ✓ 1m candles: 90 days sliding window (cleanup daily)"
