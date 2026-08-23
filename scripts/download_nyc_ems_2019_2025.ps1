# ==============================
# NYC EMS 2019–2025 Download
# Order: 2025 → 2019
# All 31 columns
# 250,000 rows per CSV
# ==============================

$baseUrl = "https://data.cityofnewyork.us/resource/76xm-jjuj"
$downloadRoot = "D:\NYC_EMS_Data"
$batchSize = 250000

# All 31 columns
$fields = @(
    "incident_id",
    "incident_datetime",
    "initial_call_type",
    "initial_severity_level_code",
    "final_call_type",
    "final_severity_level_code",
    "first_assignment_datetime",
    "valid_dispatch_rspns_time_indc",
    "dispatch_response_seconds_qy",
    "first_activation_datetime",
    "first_on_scene_datetime",
    "valid_incident_rspns_time_indc",
    "incident_response_seconds_qy",
    "incident_travel_tm_seconds_qy",
    "first_to_hosp_datetime",
    "first_hosp_arrival_datetime",
    "incident_close_datetime",
    "held_indicator",
    "incident_disposition_code",
    "borough",
    "incident_dispatch_area",
    "zipcode",
    "policeprecinct",
    "citycouncildistrict",
    "communitydistrict",
    "communityschooldistrict",
    "congressionaldistrict",
    "reopen_indicator",
    "special_event_indicator",
    "standby_indicator",
    "transfer_indicator"
) -join ","

New-Item -ItemType Directory -Path $downloadRoot -Force | Out-Null

# Download from 2025 down to 2019
foreach ($year in (2025..2019)) {

    $nextYear = $year + 1
    $yearFolder = Join-Path $downloadRoot "year=$year"

    New-Item -ItemType Directory -Path $yearFolder -Force | Out-Null

    $whereClause = "incident_datetime >= '${year}-01-01T00:00:00' AND incident_datetime < '${nextYear}-01-01T00:00:00'"

    Write-Host ""
    Write-Host "========================================"
    Write-Host "Checking total rows for $year..."
    Write-Host "========================================"

    # Get exact row count for the year
    $encodedSelect = [uri]::EscapeDataString("count(*) as total")
    $encodedWhere = [uri]::EscapeDataString($whereClause)

    $countUrl = "${baseUrl}.json?`$select=$encodedSelect&`$where=$encodedWhere"
    $countResult = Invoke-RestMethod -Uri $countUrl
    $totalRows = [int64]$countResult[0].total

    $numberOfBatches = [math]::Ceiling($totalRows / $batchSize)

    Write-Host "$year total rows: $totalRows"
    Write-Host "Number of files: $numberOfBatches"

    for ($batch = 0; $batch -lt $numberOfBatches; $batch++) {

        $offset = $batch * $batchSize
        $partNumber = ($batch + 1).ToString("000")

        $outputFile = Join-Path `
            $yearFolder `
            "ems_${year}_part_${partNumber}.csv"

        $temporaryFile = "$outputFile.part"

        # Skip files that have already downloaded successfully
        if (Test-Path $outputFile) {
            Write-Host "Already exists, skipping: $outputFile"
            continue
        }

        Write-Host ""
        Write-Host "Downloading year $year, part $partNumber..."
        Write-Host "Offset: $offset / Total: $totalRows"

        & curl.exe --get "${baseUrl}.csv" `
            --data-urlencode "`$select=$fields" `
            --data-urlencode "`$where=$whereClause" `
            --data-urlencode '$order=incident_datetime,incident_id' `
            --data-urlencode "`$limit=$batchSize" `
            --data-urlencode "`$offset=$offset" `
            --location `
            --fail `
            --retry 5 `
            --retry-delay 10 `
            --retry-all-errors `
            --connect-timeout 60 `
            --output $temporaryFile

        if ($LASTEXITCODE -eq 0) {
            Move-Item -Path $temporaryFile `
                      -Destination $outputFile `
                      -Force

            $fileSizeMB = [math]::Round(
                (Get-Item $outputFile).Length / 1MB,
                2
            )

            Write-Host "Completed: $outputFile"
            Write-Host "File size: $fileSizeMB MB"
        }
        else {
            Write-Host "Download failed: year $year, part $partNumber"
            Write-Host "Run the script again to retry."
            throw "Download stopped because curl returned an error."
        }
    }

    Write-Host ""
    Write-Host "Finished all data for $year"
}

Write-Host ""
Write-Host "========================================"
Write-Host "All downloads completed: 2025 to 2019"
Write-Host "Saved in: $downloadRoot"
Write-Host "========================================"