<#
.SYNOPSIS
    step1_html_to_csv.ps1 - Snapchat Memories HTML to CSV Converter (Windows Version)
    OPTIMIZED v2.0 - Matches Linux/Mac functionality

.DESCRIPTION
    Converts a Snapchat Memories HTML export into CSV format.
    Parses HTML tables to extract date, media type, lat/lon, and download URLs.

.PARAMETER input
    Path to Snapchat HTML export file

.PARAMETER output
    Path to output CSV file

.EXAMPLE
    .\step1_html_to_csv.ps1 --input memories_history.html --output snap_data.csv
#>

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Write-Banner {
    Write-Host ""
    Write-Host ([char]0x2554 + ([string][char]0x2550 * 63) + [char]0x2557) -ForegroundColor Cyan
    Write-Host ([char]0x2551 + (" " * 63) + [char]0x2551) -ForegroundColor Cyan
    Write-Host ([char]0x2551 + "      HTML to CSV Converter  v2.0                              " + [char]0x2551) -ForegroundColor Cyan
    Write-Host ([char]0x2551 + "      Snapchat Memories Parser (Optimized)                     " + [char]0x2551) -ForegroundColor Cyan
    Write-Host ([char]0x2551 + (" " * 63) + [char]0x2551) -ForegroundColor Cyan
    Write-Host ([char]0x255A + ([string][char]0x2550 * 63) + [char]0x255D) -ForegroundColor Cyan
    Write-Host ""
}

function Format-FileSize {
    param([long]$Bytes)
    
    if ($Bytes -ge 1MB) {
        return "{0:N1} MB" -f ($Bytes / 1MB)
    }
    elseif ($Bytes -ge 1KB) {
        return "{0:N1} KB" -f ($Bytes / 1KB)
    }
    else {
        return "$Bytes bytes"
    }
}

function Format-ElapsedTime {
    param([double]$Seconds)
    
    if ($Seconds -lt 60) {
        return "{0:N0}s" -f $Seconds
    }
    else {
        $mins = [math]::Floor($Seconds / 60)
        $secs = $Seconds % 60
        return "{0}m {1:N0}s" -f $mins, $secs
    }
}

function New-Guid4 {
    return [System.Guid]::NewGuid().ToString()
}

function Decode-HtmlEntities {
    param([string]$Text)
    
    if ([string]::IsNullOrEmpty($Text)) {
        return $Text
    }
    
    return [System.Net.WebUtility]::HtmlDecode($Text)
}

function Escape-CsvField {
    param([string]$Field)
    
    if ([string]::IsNullOrEmpty($Field)) {
        return $Field
    }
    
    if ($Field -match '[,"\r\n]') {
        $Field = $Field -replace '"', '""'
        return "`"$Field`""
    }
    return $Field
}

function Strip-HtmlTags {
    param([string]$Html)
    
    if ([string]::IsNullOrEmpty($Html)) {
        return ""
    }
    
    return ($Html -replace '<[^>]+>', '').Trim()
}

function Show-Help {
    Write-Host "step1_html_to_csv.ps1 - Snapchat Memories HTML to CSV Converter"
    Write-Host ""
    Write-Host "USAGE:"
    Write-Host "    .\step1_html_to_csv.ps1 --input [path] --output [path]"
    Write-Host ""
    Write-Host "ARGUMENTS:"
    Write-Host "    --input [path]      Path to Snapchat HTML export file (required)"
    Write-Host "    --output [path]     Path to output CSV file (required)"
    Write-Host "    --help              Show this help message"
    Write-Host ""
    Write-Host "EXAMPLE:"
    Write-Host "    .\step1_html_to_csv.ps1 --input memories_history.html --output snap_data.csv"
}

# ============================================================================
# ARGUMENT PARSING
# ============================================================================

$InputFile = $null
$OutputFile = $null

$i = 0
while ($i -lt $args.Count) {
    $arg = $args[$i]
    switch -Regex ($arg) {
        "^--input$" {
            if ($i + 1 -lt $args.Count) {
                $InputFile = $args[$i + 1]
                $i++
            }
        }
        "^--output$" {
            if ($i + 1 -lt $args.Count) {
                $OutputFile = $args[$i + 1]
                $i++
            }
        }
        "^--help$|^-h$|^-\?$" {
            Show-Help
            exit 0
        }
    }
    $i++
}

# Validate required arguments
if ([string]::IsNullOrEmpty($InputFile)) {
    Write-Host "ERROR: --input argument is required. Use --help for usage." -ForegroundColor Red
    exit 1
}
if ([string]::IsNullOrEmpty($OutputFile)) {
    Write-Host "ERROR: --output argument is required. Use --help for usage." -ForegroundColor Red
    exit 1
}

# Convert to absolute paths
if (-not [System.IO.Path]::IsPathRooted($InputFile)) {
    $InputFile = Join-Path -Path (Get-Location).Path -ChildPath $InputFile
}
if (-not [System.IO.Path]::IsPathRooted($OutputFile)) {
    $OutputFile = Join-Path -Path (Get-Location).Path -ChildPath $OutputFile
}

# Validate input file
if (-not (Test-Path $InputFile)) {
    Write-Host "ERROR: Input file not found: $InputFile" -ForegroundColor Red
    exit 1
}

# Create output directory if needed
$OutputDir = Split-Path $OutputFile -Parent
if (-not [string]::IsNullOrEmpty($OutputDir) -and -not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

# ============================================================================
# MAIN PROCESSING
# ============================================================================

Write-Banner

Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Input:   $InputFile"
Write-Host "  Output:  $OutputFile"

# Get file info
$fileInfo = Get-Item $InputFile
Write-Host "  Size:    $(Format-FileSize $fileInfo.Length)"
Write-Host ""

Write-Host "Analyzing HTML structure..." -ForegroundColor Cyan
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Read the entire HTML file
$htmlContent = Get-Content -Path $InputFile -Raw -Encoding UTF8

# Count approximate rows
$rowCount = ([regex]::Matches($htmlContent, '<tr')).Count
Write-Host "  Found approximately " -NoNewline
Write-Host "$rowCount" -ForegroundColor White -NoNewline
Write-Host " table rows to process"
Write-Host ""

# Warn for large files
if ($fileInfo.Length -gt 50MB) {
    Write-Host "Warning: Large file detected. Processing may take a moment..." -ForegroundColor Yellow
    Write-Host ""
}

Write-Host ("-" * 65) -ForegroundColor DarkGray
Write-Host "Starting conversion..." -ForegroundColor White
Write-Host ("-" * 65) -ForegroundColor DarkGray
Write-Host ""

# Normalize whitespace
$htmlContent = $htmlContent -replace '[\r\n]+', ' '

# Extract all table rows
$rowPattern = '<tr[^>]*>(.*?)</tr>'
$rows = [regex]::Matches($htmlContent, $rowPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Singleline)

$rowsWritten = 0
$rowsSkipped = 0
$csvLines = [System.Collections.ArrayList]@()

# Add header
$null = $csvLines.Add("id,date_utc,media_type,latitude,longitude,download_url")

$totalRows = $rows.Count
$processedCount = 0

foreach ($row in $rows) {
    $processedCount++
    
    # Show progress every 100 rows
    if ($processedCount % 100 -eq 0) {
        $pct = [math]::Round(($processedCount / $totalRows) * 100)
        Write-Host ("`r  Processing: $processedCount/$totalRows rows ($pct" + "%)... ") -NoNewline -ForegroundColor Gray
    }
    
    $rowContent = $row.Groups[1].Value
    
    # Extract all <td> contents
    $tdPattern = '<td[^>]*>(.*?)</td>'
    $cells = [regex]::Matches($rowContent, $tdPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Singleline)
    
    # Need at least 3 cells
    if ($cells.Count -lt 3) {
        continue
    }
    
    # Extract fields
    $dateUtc = Strip-HtmlTags -Html $cells[0].Groups[1].Value
    $mediaType = Strip-HtmlTags -Html $cells[1].Groups[1].Value
    
    # Only process Image or Video
    if ($mediaType -ne "Image" -and $mediaType -ne "Video") {
        continue
    }
    
    # Extract lat/lon
    $location = Strip-HtmlTags -Html $cells[2].Groups[1].Value
    $latitude = ""
    $longitude = ""
    
    $latLonPattern = 'Latitude\s*,\s*Longitude\s*:\s*([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)'
    $latLonMatch = [regex]::Match($location, $latLonPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($latLonMatch.Success) {
        $latitude = $latLonMatch.Groups[1].Value
        $longitude = $latLonMatch.Groups[2].Value
    }
    
    # Extract download URL
    $downloadUrl = ""
    $urlPatterns = @(
        "downloadMemories\s*\(\s*'([^']+)'",
        'downloadMemories\s*\(\s*"([^"]+)"'
    )
    
    foreach ($pattern in $urlPatterns) {
        $urlMatch = [regex]::Match($rowContent, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($urlMatch.Success) {
            $downloadUrl = $urlMatch.Groups[1].Value
            break
        }
    }
    
    # Try HTML entity encoded patterns if not found
    if ([string]::IsNullOrEmpty($downloadUrl)) {
        $ampQuotPattern = 'downloadMemories\s*\(\s*&quot;([^&]+)&quot;'
        $urlMatch = [regex]::Match($rowContent, $ampQuotPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($urlMatch.Success) {
            $downloadUrl = $urlMatch.Groups[1].Value
        }
    }
    
    $downloadUrl = Decode-HtmlEntities -Text $downloadUrl
    
    if ([string]::IsNullOrEmpty($downloadUrl)) {
        $rowsSkipped++
        continue
    }
    
    # Generate UUID and build CSV line
    $rowId = New-Guid4
    
    $csvLine = @(
        (Escape-CsvField -Field $rowId),
        (Escape-CsvField -Field $dateUtc),
        (Escape-CsvField -Field $mediaType),
        (Escape-CsvField -Field $latitude),
        (Escape-CsvField -Field $longitude),
        (Escape-CsvField -Field $downloadUrl)
    ) -join ","
    
    $null = $csvLines.Add($csvLine)
    $rowsWritten++
}

# Clear progress line
Write-Host ("`r" + (" " * 60) + "`r") -NoNewline

# Write to file
$csvLines | Out-File -FilePath $OutputFile -Encoding UTF8 -Force

$stopwatch.Stop()
$elapsed = $stopwatch.Elapsed.TotalSeconds

Write-Host ""
Write-Host ("=" * 65) -ForegroundColor DarkGray
Write-Host "                    CONVERSION COMPLETE                        " -ForegroundColor Green
Write-Host ("=" * 65) -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Rows written:  " -NoNewline -ForegroundColor White
Write-Host "$rowsWritten" -ForegroundColor Green
if ($rowsSkipped -gt 0) {
    Write-Host "  Rows skipped:  " -NoNewline -ForegroundColor White
    Write-Host "$rowsSkipped" -ForegroundColor Yellow -NoNewline
    Write-Host " (missing download URL)"
}
Write-Host "  Time elapsed:  $(Format-ElapsedTime $elapsed)" -ForegroundColor White
if ($elapsed -gt 0 -and $rowsWritten -gt 0) {
    $speed = [math]::Round($rowsWritten / $elapsed)
    if ($speed -gt 0) {
        Write-Host "  Speed:         $speed rows/sec" -ForegroundColor White
    }
}
Write-Host ""
Write-Host "  Output file:   " -NoNewline -ForegroundColor White
Write-Host "$OutputFile" -ForegroundColor Cyan
Write-Host ""
