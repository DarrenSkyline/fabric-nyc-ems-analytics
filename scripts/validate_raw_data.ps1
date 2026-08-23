$ErrorActionPreference = "Stop"

# ==========================================
# NYC EMS Raw Data Validation
# ==========================================

$dataRoot = "D:\NYC_EMS_Data"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$outputFile = Join-Path $projectRoot "docs\raw-data-inventory.md"

$expectedYears = 2025..2019
$expectedColumnCount = 31
$baseUrl = "https://data.cityofnewyork.us/resource/76xm-jjuj"

$canonicalHeader = $null
$summary = @()
$fileDetails = @()

Write-Host "NYC EMS raw data validation started."
Write-Host "Data location: $dataRoot"
Write-Host ""

foreach ($year in $expectedYears) {

    $yearFolder = Join-Path $dataRoot "year=$year"

    Write-Host "======================================="
    Write-Host "Validating year $year"
    Write-Host "======================================="

    if (-not (Test-Path $yearFolder)) {
        Write-Host "ERROR: Missing folder $yearFolder"

        $summary += [PSCustomObject]@{
            Year             = $year
            CsvFiles         = 0
            LocalRows        = 0
            SourceRows       = 0
            TotalGB          = 0
            PartFiles        = 0
            EmptyFiles       = 0
            SchemaErrors     = 0
            HeaderErrors     = 0
            ValidationStatus = "MISSING FOLDER"
        }

        continue
    }

    $csvFiles = @(
        Get-ChildItem $yearFolder -File -Filter "*.csv" |
        Sort-Object Name
    )

    $partFiles = @(
        Get-ChildItem $yearFolder -File -Filter "*.part"
    )

    $localRows = [int64]0
    $totalBytes = [int64]0
    $emptyFileCount = 0
    $schemaErrorCount = 0
    $headerErrorCount = 0

    foreach ($file in $csvFiles) {

        Write-Host "Checking $($file.Name)..."

        $totalBytes += $file.Length

        if ($file.Length -eq 0) {
            $emptyFileCount++

            $fileDetails += [PSCustomObject]@{
                Year          = $year
                File          = $file.Name
                Rows          = 0
                Columns       = 0
                SizeMB        = 0
                SchemaStatus  = "EMPTY FILE"
                HeaderStatus  = "NOT CHECKED"
            }

            continue
        }

        $reader = [System.IO.StreamReader]::new($file.FullName)

        try {
            $header = $reader.ReadLine()

            if ([string]::IsNullOrWhiteSpace($header)) {
                $emptyFileCount++
                $columnCount = 0
                $headerStatus = "MISSING HEADER"
                $schemaStatus = "INVALID"
                $rowCount = 0
            }
            else {
                $columnCount = ($header -split ",").Count

                if ($null -eq $canonicalHeader) {
                    $canonicalHeader = $header
                }

                if ($header -eq $canonicalHeader) {
                    $headerStatus = "PASS"
                }
                else {
                    $headerStatus = "FAIL"
                    $headerErrorCount++
                }

                if ($columnCount -eq $expectedColumnCount) {
                    $schemaStatus = "PASS"
                }
                else {
                    $schemaStatus = "FAIL"
                    $schemaErrorCount++
                }

                $rowCount = [int64]0

                while (($line = $reader.ReadLine()) -ne $null) {
                    if (-not [string]::IsNullOrWhiteSpace($line)) {
                        $rowCount++
                    }
                }
            }
        }
        finally {
            $reader.Close()
            $reader.Dispose()
        }

        $localRows += $rowCount

        $fileDetails += [PSCustomObject]@{
            Year          = $year
            File          = $file.Name
            Rows          = $rowCount
            Columns       = $columnCount
            SizeMB        = [math]::Round($file.Length / 1MB, 2)
            SchemaStatus  = $schemaStatus
            HeaderStatus  = $headerStatus
        }
    }

    # Query the current official row count from Socrata
    $nextYear = $year + 1

    $whereClause = "incident_datetime >= '${year}-01-01T00:00:00' AND incident_datetime < '${nextYear}-01-01T00:00:00'"

    $encodedSelect = [uri]::EscapeDataString("count(*) as total")
    $encodedWhere = [uri]::EscapeDataString($whereClause)

    $countUrl = "${baseUrl}.json?`$select=$encodedSelect&`$where=$encodedWhere"

    try {
        $countResult = Invoke-RestMethod -Uri $countUrl
        $sourceRows = [int64]$countResult[0].total
    }
    catch {
        Write-Host "WARNING: Could not retrieve Socrata row count."
        $sourceRows = -1
    }

    if ($csvFiles.Count -eq 0) {
        $validationStatus = "NO CSV FILES"
    }
    elseif ($partFiles.Count -gt 0) {
        $validationStatus = "PART FILE FOUND"
    }
    elseif ($emptyFileCount -gt 0) {
        $validationStatus = "EMPTY FILE FOUND"
    }
    elseif ($schemaErrorCount -gt 0) {
        $validationStatus = "SCHEMA ERROR"
    }
    elseif ($headerErrorCount -gt 0) {
        $validationStatus = "HEADER ERROR"
    }
    elseif (($sourceRows -ge 0) -and ($localRows -ne $sourceRows)) {
        $validationStatus = "ROW COUNT MISMATCH"
    }
    else {
        $validationStatus = "PASS"
    }

    $summary += [PSCustomObject]@{
        Year             = $year
        CsvFiles         = $csvFiles.Count
        LocalRows        = $localRows
        SourceRows       = $sourceRows
        TotalGB          = [math]::Round($totalBytes / 1GB, 3)
        PartFiles        = $partFiles.Count
        EmptyFiles       = $emptyFileCount
        SchemaErrors     = $schemaErrorCount
        HeaderErrors     = $headerErrorCount
        ValidationStatus = $validationStatus
    }

    Write-Host "$year completed: $validationStatus"
    Write-Host ""
}

# ==========================================
# Generate Markdown inventory
# ==========================================

$markdown = [System.Collections.Generic.List[string]]::new()

$markdown.Add("# NYC EMS Raw Data Inventory")
$markdown.Add("")
$markdown.Add("## Dataset")
$markdown.Add("")
$markdown.Add("- Source: NYC Open Data")
$markdown.Add("- Dataset: EMS Incident Dispatch Data")
$markdown.Add("- Dataset ID: ``76xm-jjuj``")
$markdown.Add("- Reporting period: 2019–2025")
$markdown.Add("- Expected columns: 31")
$markdown.Add("- Local raw data path: ``D:\NYC_EMS_Data``")
$markdown.Add("")
$markdown.Add("## Validation Summary")
$markdown.Add("")
$markdown.Add("| Year | CSV Files | Local Rows | Source Rows | Size GB | Part Files | Empty Files | Schema Errors | Header Errors | Status |")
$markdown.Add("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")

foreach ($item in $summary) {
    $markdown.Add(
        "| $($item.Year) | $($item.CsvFiles) | $($item.LocalRows) | $($item.SourceRows) | $($item.TotalGB) | $($item.PartFiles) | $($item.EmptyFiles) | $($item.SchemaErrors) | $($item.HeaderErrors) | $($item.ValidationStatus) |"
    )
}

$totalLocalRows = ($summary | Measure-Object LocalRows -Sum).Sum
$totalSourceRows = ($summary | Where-Object SourceRows -GE 0 |
    Measure-Object SourceRows -Sum).Sum
$totalSizeGB = [math]::Round(
    ($summary | Measure-Object TotalGB -Sum).Sum,
    3
)

$markdown.Add("")
$markdown.Add("## Overall Totals")
$markdown.Add("")
$markdown.Add("- Local rows: $totalLocalRows")
$markdown.Add("- Source rows: $totalSourceRows")
$markdown.Add("- Total raw CSV size: $totalSizeGB GB")
$markdown.Add("")

$failedFiles = @(
    $fileDetails |
    Where-Object {
        $_.SchemaStatus -ne "PASS" -or
        $_.HeaderStatus -ne "PASS"
    }
)

if ($failedFiles.Count -eq 0) {
    $markdown.Add("## File Schema Validation")
    $markdown.Add("")
    $markdown.Add("All CSV files contain 31 columns and use a consistent header.")
}
else {
    $markdown.Add("## Files Requiring Investigation")
    $markdown.Add("")
    $markdown.Add("| Year | File | Rows | Columns | Schema | Header |")
    $markdown.Add("|---:|---|---:|---:|---|---|")

    foreach ($file in $failedFiles) {
        $markdown.Add(
            "| $($file.Year) | $($file.File) | $($file.Rows) | $($file.Columns) | $($file.SchemaStatus) | $($file.HeaderStatus) |"
        )
    }
}

$markdown.Add("")
$markdown.Add("## Validation Notes")
$markdown.Add("")
$markdown.Add("- Each CSV file contains its own header row.")
$markdown.Add("- Header rows are excluded from local row counts.")
$markdown.Add("- Source row counts are queried from the Socrata API.")
$markdown.Add("- Raw CSV files are excluded from GitHub.")
$markdown.Add("- Exact row counts will be validated again after Bronze ingestion.")

$markdown | Set-Content -Path $outputFile -Encoding UTF8

Write-Host ""
Write-Host "======================================="
Write-Host "VALIDATION SUMMARY"
Write-Host "======================================="

$summary |
    Format-Table Year, CsvFiles, LocalRows, SourceRows,
        TotalGB, PartFiles, SchemaErrors,
        HeaderErrors, ValidationStatus -AutoSize

Write-Host ""
Write-Host "Inventory generated:"
Write-Host $outputFile