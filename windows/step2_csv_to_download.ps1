<#
.SYNOPSIS
    SSLAXSnap v3.0 - Snapchat Memories Downloader

.DESCRIPTION
    Downloads Snapchat Memories from a CSV file with async batch processing,
    magic byte file type detection, and atomic CSV updates.

.PARAMETER input
    Path to CSV input file

.PARAMETER outdir
    Root output folder for downloaded files

.PARAMETER batch
    Max concurrent downloads (default: 5)

.PARAMETER cooldown
    Delay in seconds after each completed batch (default: 3)

.EXAMPLE
    .\SSLAXSnap_v3.ps1 --input snap_data.csv --outdir C:\Downloads\Snaps --batch 10 --cooldown 5
#>

# ============================================================================
# ARGUMENT PARSING (supports --arg style)
# ============================================================================

# Default values
$InputFile = $null
$OutputDir = $null
$BatchSize = 5
$CooldownSeconds = 3

# Parse arguments
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
        "^--outdir$" {
            if ($i + 1 -lt $args.Count) {
                $OutputDir = $args[$i + 1]
                $i++
            }
        }
        "^--batch$" {
            if ($i + 1 -lt $args.Count) {
                $BatchSize = [int]$args[$i + 1]
                $i++
            }
        }
        "^--cooldown$" {
            if ($i + 1 -lt $args.Count) {
                $CooldownSeconds = [int]$args[$i + 1]
                $i++
            }
        }
        "^--help$|^-h$|^-\?$" {
            Write-Host @"
SSLAXSnap v3.0 - Snapchat Memories Downloader

USAGE:
    .\SSLAXSnap_v3.ps1 --input <path> --outdir <path> [--batch <int>] [--cooldown <seconds>]

ARGUMENTS:
    --input <path>      Path to CSV input file (required)
    --outdir <path>     Root output folder for downloaded files (required)
    --batch <int>       Max concurrent downloads (default: 5)
    --cooldown <sec>    Delay in seconds after each batch (default: 3)
    --help              Show this help message

EXAMPLE:
    .\SSLAXSnap_v3.ps1 --input snap_data.csv --outdir C:\Downloads\Snaps --batch 10 --cooldown 5
"@
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
if ([string]::IsNullOrEmpty($OutputDir)) {
    Write-Host "ERROR: --outdir argument is required. Use --help for usage." -ForegroundColor Red
    exit 1
}

# ============================================================================
# BANNER
# ============================================================================
function Write-Banner {
    $banner = @"

+===========================================================================+
|                                                                           |
|   SSLAXSNAP v3.0 - Snapchat Memories Batch Downloader                    |
|                                                                           |
+===========================================================================+

"@
    Write-Host $banner -ForegroundColor Cyan
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Get-FileFormatFromMagicBytes {
    <#
    .SYNOPSIS
        Detects file format from magic bytes
    .RETURNS
        "zip", "png", "jpeg", or "mp4" (actual format identifiers)
    #>
    param([string]$FilePath)
    
    try {
        if (-not (Test-Path $FilePath)) {
            return "jpeg"  # Default to jpeg if file doesn't exist
        }
        
        $fileInfo = Get-Item $FilePath
        if ($fileInfo.Length -lt 16) {
            return "jpeg"  # File too small, default to jpeg
        }
        
        # Read first 16 bytes
        $stream = [System.IO.File]::OpenRead($FilePath)
        $bytes = New-Object byte[] 16
        $bytesRead = $stream.Read($bytes, 0, 16)
        $stream.Close()
        
        if ($bytesRead -lt 2) {
            return "jpeg"
        }
        
        # ZIP: starts with PK (0x50 0x4B)
        if ($bytes[0] -eq 0x50 -and $bytes[1] -eq 0x4B) {
            return "zip"
        }
        
        # PNG: 89 50 4E 47 0D 0A 1A 0A
        if ($bytesRead -ge 8 -and 
            $bytes[0] -eq 0x89 -and $bytes[1] -eq 0x50 -and $bytes[2] -eq 0x4E -and 
            $bytes[3] -eq 0x47 -and $bytes[4] -eq 0x0D -and $bytes[5] -eq 0x0A -and
            $bytes[6] -eq 0x1A -and $bytes[7] -eq 0x0A) {
            return "png"
        }
        
        # JPEG: FF D8 FF
        if ($bytesRead -ge 3 -and 
            $bytes[0] -eq 0xFF -and $bytes[1] -eq 0xD8 -and $bytes[2] -eq 0xFF) {
            return "jpeg"
        }
        
        # MP4: "ftyp" within first 16 bytes
        if ($bytesRead -ge 8) {
            $ascii = [System.Text.Encoding]::ASCII.GetString($bytes)
            if ($ascii -match "ftyp") {
                return "mp4"
            }
        }
        
        # Default: treat as JPEG
        return "jpeg"
    }
    catch {
        return "jpeg"  # Default on error
    }
}

function Get-FileExtension {
    <#
    .SYNOPSIS
        Returns the file extension for a given file format
    #>
    param([string]$FileFormat)
    
    switch ($FileFormat) {
        "zip"  { return ".zip" }
        "png"  { return ".png" }
        "mp4"  { return ".mp4" }
        default { return ".jpg" }
    }
}

function Get-TypeLabel {
    <#
    .SYNOPSIS
        Returns the type label for the filename based on format
        jpeg/png -> image, mp4 -> video, zip -> filter_archive
    #>
    param([string]$FileFormat)
    
    switch ($FileFormat) {
        "zip" { return "filter_archive" }
        "mp4" { return "video" }
        default { return "image" }
    }
}

function Format-OutputFileName {
    <#
    .SYNOPSIS
        Builds the output filename according to the required format
    .DESCRIPTION
        Format: DD.MM.YYYY_HHMMSS_[lat_XX_long_YY]_####_type.ext
    #>
    param(
        [string]$DateUtc,
        [string]$Latitude,
        [string]$Longitude,
        [int]$SequenceNumber,
        [string]$FileType
    )
    
    # Parse date: "2025-12-16 10:16:16 UTC"
    try {
        $datePart = $DateUtc -replace " UTC$", ""
        $dt = [datetime]::ParseExact($datePart, "yyyy-MM-dd HH:mm:ss", $null)
        $timestamp = $dt.ToString("dd.MM.yyyy_HHmmss")
    }
    catch {
        # Fallback: try to extract manually
        if ($DateUtc -match "(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})") {
            $year = $matches[1]
            $month = $matches[2]
            $day = $matches[3]
            $hour = $matches[4]
            $minute = $matches[5]
            $second = $matches[6]
            $timestamp = "${day}.${month}.${year}_${hour}${minute}${second}"
        }
        else {
            $timestamp = "00.00.0000_000000"
        }
    }
    
    # Format lat/long
    $latStr = if ([string]::IsNullOrWhiteSpace($Latitude)) { "" } else { $Latitude }
    $longStr = if ([string]::IsNullOrWhiteSpace($Longitude)) { "" } else { $Longitude }
    $coords = "[lat_${latStr}_long_${longStr}]"
    
    # Sequence number (4 digits, zero-padded)
    $seq = $SequenceNumber.ToString("D4")
    
    # Type label and extension
    $typeLabel = Get-TypeLabel -FileType $FileType
    $ext = Get-FileExtension -FileType $FileType
    
    return "${timestamp}_${coords}_${seq}_${typeLabel}${ext}"
}

function Get-DateFolder {
    <#
    .SYNOPSIS
        Returns the dated subfolder path for a given date
    .DESCRIPTION
        Format: YYYY\MM MonthName\DD.MM.YYYY (e.g., 2025\12 December\15.12.2025)
    #>
    param([string]$DateUtc)
    
    $monthNames = @('', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
    
    try {
        $datePart = $DateUtc -replace " UTC$", ""
        $dt = [datetime]::ParseExact($datePart, "yyyy-MM-dd HH:mm:ss", $null)
        $year = $dt.Year.ToString("D4")
        $month = $dt.Month.ToString("D2")
        $day = $dt.Day.ToString("D2")
        $monthName = $monthNames[$dt.Month]
        return Join-Path $year (Join-Path "$month $monthName" "$day.$month.$year")
    }
    catch {
        if ($DateUtc -match "(\d{4})-(\d{2})-(\d{2})") {
            $year = $matches[1]
            $month = $matches[2]
            $day = $matches[3]
            $monthNum = [int]$month
            $monthName = $monthNames[$monthNum]
            return Join-Path $year (Join-Path "$month $monthName" "$day.$month.$year")
        }
        return "0000\00 Unknown\00.00.0000"
    }
}

function Save-CsvAtomically {
    <#
    .SYNOPSIS
        Saves CSV data atomically using temp file + rename strategy
    #>
    param(
        [string]$CsvPath,
        [array]$Rows
    )
    
    if ($null -eq $Rows -or $Rows.Count -eq 0) {
        if (Test-Path $CsvPath) { Remove-Item $CsvPath -Force }
        return $true
    }
    
    $tempPath = "${CsvPath}.tmp"
    
    try {
        $sw = New-Object System.IO.StreamWriter($tempPath, $false, [System.Text.Encoding]::UTF8)
        
        # Write header
        $sw.WriteLine("id,date_utc,media_type,latitude,longitude,download_url")
        
        foreach ($row in $Rows) {
            # Escape fields
            $fields = @(
                $row.id,
                $row.date_utc,
                $row.media_type,
                $row.latitude,
                $row.longitude,
                $row.download_url
            ) | ForEach-Object {
                $field = if ($null -eq $_) { "" } else { $_.ToString() }
                if ($field -match '[,"\r\n]') {
                    '"' + ($field -replace '"', '""') + '"'
                }
                else {
                    $field
                }
            }
            $sw.WriteLine(($fields -join ","))
        }
        
        $sw.Close()
        $sw.Dispose()
        
        # Atomic move
        if (Test-Path $CsvPath) { Remove-Item $CsvPath -Force }
        Move-Item -Path $tempPath -Destination $CsvPath -Force
        
        return $true
    }
    catch {
        if ($sw) { $sw.Dispose() }
        if (Test-Path $tempPath) {
            Remove-Item $tempPath -Force -ErrorAction SilentlyContinue
        }
        return $false
    }
}

function Format-ElapsedTime {
    param([double]$Seconds)
    
    if ($Seconds -lt 1) {
        return "{0:N0}ms" -f ($Seconds * 1000)
    }
    elseif ($Seconds -lt 60) {
        return "{0:N1}s" -f $Seconds
    }
    else {
        $mins = [math]::Floor($Seconds / 60)
        $secs = $Seconds % 60
        return "{0}m {1:N0}s" -f $mins, $secs
    }
}

# ============================================================================
# DOWNLOAD JOB SCRIPTBLOCK
# ============================================================================

$DownloadScriptBlock = {
    param(
        [string]$Id,
        [string]$DateUtc,
        [string]$MediaType,
        [string]$Latitude,
        [string]$Longitude,
        [string]$DownloadUrl,
        [string]$OutputDir,
        [int]$SequenceNumber
    )
    
    $result = @{
        Id = $Id
        Success = $false
        Error = $null
        HttpStatus = $null
        OutputFile = $null
        FileType = $null
        ElapsedMs = 0
    }
    
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    
    try {
        # Build date folder path (format: YYYY\MM MonthName\DD.MM.YYYY)
        $monthNames = @('', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
        $datePart = $DateUtc -replace " UTC$", ""
        try {
            $dt = [datetime]::ParseExact($datePart, "yyyy-MM-dd HH:mm:ss", $null)
            $year = $dt.Year.ToString("D4")
            $month = $dt.Month.ToString("D2")
            $day = $dt.Day.ToString("D2")
            $monthName = $monthNames[$dt.Month]
            $dateFolder = Join-Path $year (Join-Path "$month $monthName" "$day.$month.$year")
        }
        catch {
            if ($DateUtc -match "(\d{4})-(\d{2})-(\d{2})") {
                $year = $matches[1]
                $month = $matches[2]
                $day = $matches[3]
                $monthNum = [int]$month
                $monthName = $monthNames[$monthNum]
                $dateFolder = Join-Path $year (Join-Path "$month $monthName" "$day.$month.$year")
            }
            else {
                $dateFolder = "0000\00 Unknown\00.00.0000"
            }
        }
        
        $targetDir = Join-Path $OutputDir $dateFolder
        
        # Create directory if needed
        if (-not (Test-Path $targetDir)) {
            New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
        }
        
        # Temp file path
        $tempFile = Join-Path $targetDir "${Id}.tmp"
        
        # Download using curl.exe
        $curlArgs = @(
            "-s",            # Silent
            "-S",            # Show errors
            "-L",            # Follow redirects
            "--connect-timeout", "15",
            "--max-time", "300",
            "-o", $tempFile,
            "-w", "%{http_code}",
            $DownloadUrl
        )
        
        $httpCode = & curl.exe @curlArgs 2>&1
        $curlExitCode = $LASTEXITCODE
        
        # Extract HTTP status code (last line of output)
        if ($httpCode -is [array]) {
            $result.HttpStatus = $httpCode[-1]
        }
        else {
            $result.HttpStatus = $httpCode
        }
        
        # Check for curl errors
        if ($curlExitCode -ne 0) {
            $result.Error = "curl failed with exit code $curlExitCode"
            if (Test-Path $tempFile) { Remove-Item $tempFile -Force -ErrorAction SilentlyContinue }
            $stopwatch.Stop()
            $result.ElapsedMs = $stopwatch.ElapsedMilliseconds
            return $result
        }
        
        # Check HTTP status
        if ($result.HttpStatus -notmatch "^2\d{2}$") {
            $result.Error = "HTTP error: $($result.HttpStatus)"
            if (Test-Path $tempFile) { Remove-Item $tempFile -Force -ErrorAction SilentlyContinue }
            $stopwatch.Stop()
            $result.ElapsedMs = $stopwatch.ElapsedMilliseconds
            return $result
        }
        
        # Verify file exists
        if (-not (Test-Path $tempFile)) {
            $result.Error = "Download file not created"
            $stopwatch.Stop()
            $result.ElapsedMs = $stopwatch.ElapsedMilliseconds
            return $result
        }
        
        # Detect file type from magic bytes
        $fileInfo = Get-Item $tempFile
        if ($fileInfo.Length -lt 2) {
            $result.Error = "Downloaded file is too small"
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
            $stopwatch.Stop()
            $result.ElapsedMs = $stopwatch.ElapsedMilliseconds
            return $result
        }
        
        $stream = [System.IO.File]::OpenRead($tempFile)
        $bytes = New-Object byte[] 16
        $bytesRead = $stream.Read($bytes, 0, 16)
        $stream.Close()
        
        $fileFormat = "jpeg"  # Default
        
        if ($bytesRead -ge 2 -and $bytes[0] -eq 0x50 -and $bytes[1] -eq 0x4B) {
            $fileFormat = "zip"
        }
        elseif ($bytesRead -ge 8 -and 
                $bytes[0] -eq 0x89 -and $bytes[1] -eq 0x50 -and $bytes[2] -eq 0x4E -and 
                $bytes[3] -eq 0x47 -and $bytes[4] -eq 0x0D -and $bytes[5] -eq 0x0A -and
                $bytes[6] -eq 0x1A -and $bytes[7] -eq 0x0A) {
            $fileFormat = "png"
        }
        elseif ($bytesRead -ge 3 -and 
                $bytes[0] -eq 0xFF -and $bytes[1] -eq 0xD8 -and $bytes[2] -eq 0xFF) {
            $fileFormat = "jpeg"
        }
        elseif ($bytesRead -ge 8) {
            $ascii = [System.Text.Encoding]::ASCII.GetString($bytes)
            if ($ascii -match "ftyp") {
                $fileFormat = "mp4"
            }
        }
        
        $result.FileType = $fileFormat
        
        # Build filename
        try {
            $dt = [datetime]::ParseExact($datePart, "yyyy-MM-dd HH:mm:ss", $null)
            $timestamp = $dt.ToString("dd.MM.yyyy_HHmmss")
        }
        catch {
            if ($DateUtc -match "(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})") {
                $timestamp = "$($matches[3]).$($matches[2]).$($matches[1])_$($matches[4])$($matches[5])$($matches[6])"
            }
            else {
                $timestamp = "00.00.0000_000000"
            }
        }
        
        $latStr = if ([string]::IsNullOrWhiteSpace($Latitude)) { "" } else { $Latitude }
        $longStr = if ([string]::IsNullOrWhiteSpace($Longitude)) { "" } else { $Longitude }
        $coords = "[lat_${latStr}_long_${longStr}]"
        $seq = $SequenceNumber.ToString("D4")
        
        # Map format to label: jpeg/png -> image, mp4 -> video, zip -> filter_archive
        $typeLabel = switch ($fileFormat) {
            "zip" { "filter_archive" }
            "mp4" { "video" }
            default { "image" }
        }
        
        # Map format to extension
        $ext = switch ($fileFormat) {
            "zip" { ".zip" }
            "png" { ".png" }
            "mp4" { ".mp4" }
            default { ".jpg" }
        }
        
        $finalName = "${timestamp}_${coords}_${seq}_${typeLabel}${ext}"
        $finalPath = Join-Path $targetDir $finalName
        
        # Rename (overwrite if exists)
        Move-Item -Path $tempFile -Destination $finalPath -Force
        
        $result.Success = $true
        $result.OutputFile = $finalPath
    }
    catch {
        $result.Error = $_.Exception.Message
        # Cleanup temp file if it exists
        if ($tempFile -and (Test-Path $tempFile)) {
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
        }
    }
    finally {
        $stopwatch.Stop()
        $result.ElapsedMs = $stopwatch.ElapsedMilliseconds
    }
    
    return $result
}

# ============================================================================
# MAIN SCRIPT EXECUTION
# ============================================================================

# Show banner
Write-Banner

# Convert to absolute paths - resolve relative to current directory
if (-not [System.IO.Path]::IsPathRooted($InputFile)) {
    $InputFile = Join-Path -Path (Get-Location).Path -ChildPath $InputFile
}
if (-not [System.IO.Path]::IsPathRooted($OutputDir)) {
    $OutputDir = Join-Path -Path (Get-Location).Path -ChildPath $OutputDir
}

Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Input CSV:   $InputFile"
Write-Host "  Output Dir:  $OutputDir"
Write-Host "  Batch Size:  $BatchSize"
Write-Host "  Cooldown:    ${CooldownSeconds}s"
Write-Host ""

if (-not (Test-Path $InputFile)) {
    Write-Host "ERROR: Input file not found: $InputFile" -ForegroundColor Red
    exit 1
}

# Create output directory if needed
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    Write-Host "Created output directory: $OutputDir" -ForegroundColor Green
}

# Load CSV
Write-Host "Loading CSV..." -ForegroundColor Cyan
try {
    $csvData = Import-Csv -Path $InputFile
    $remainingRows = [System.Collections.ArrayList]@($csvData)
}
catch {
    Write-Host "ERROR: Failed to load CSV: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

$totalRows = $remainingRows.Count
if ($totalRows -eq 0) {
    Write-Host "No rows to process. CSV is empty or already complete." -ForegroundColor Yellow
    exit 0
}

Write-Host "Loaded $totalRows rows to process" -ForegroundColor Green
Write-Host ""

# Calculate batches
$totalBatches = [math]::Ceiling($totalRows / $BatchSize)

# Counters
$successCount = 0
$failedCount = 0
$sequenceCounter = 0  # Increments for each successful download
$currentBatch = 0

Write-Host ("=" * 75) -ForegroundColor DarkGray
Write-Host "Starting downloads: $totalRows items in $totalBatches batches" -ForegroundColor White
Write-Host ("=" * 75) -ForegroundColor DarkGray
Write-Host ""

$overallStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Process batches
while ($remainingRows.Count -gt 0) {
    $currentBatch++
    
    # Get items for this batch
    $batchItems = @()
    $itemsToTake = [math]::Min($BatchSize, $remainingRows.Count)
    for ($i = 0; $i -lt $itemsToTake; $i++) {
        $batchItems += $remainingRows[$i]
    }
    
    Write-Host ("-" * 75) -ForegroundColor DarkGray
    $remainingCount = $remainingRows.Count
    Write-Host ("BATCH $currentBatch/$totalBatches - Processing $($batchItems.Count) items ($remainingCount remaining)") -ForegroundColor Cyan
    Write-Host ("-" * 75) -ForegroundColor DarkGray
    
    # Start jobs for this batch
    $jobs = @()
    $batchSequenceStart = $sequenceCounter
    
    foreach ($item in $batchItems) {
        $sequenceCounter++
        
        $job = Start-Job -ScriptBlock $DownloadScriptBlock -ArgumentList @(
            $item.id,
            $item.date_utc,
            $item.media_type,
            $item.latitude,
            $item.longitude,
            $item.download_url,
            $OutputDir,
            $sequenceCounter
        )
        
        $jobs += @{
            Job = $job
            Item = $item
            Sequence = $sequenceCounter
        }
    }
    
    # Wait for all jobs to complete
    $jobObjects = $jobs | ForEach-Object { $_.Job }
    $null = Wait-Job -Job $jobObjects
    
    # Collect results
    $successfulIds = @()
    $batchSuccessCount = 0
    $batchFailCount = 0
    
    foreach ($jobInfo in $jobs) {
        $result = Receive-Job -Job $jobInfo.Job
        Remove-Job -Job $jobInfo.Job -Force
        
        $elapsed = Format-ElapsedTime -Seconds ($result.ElapsedMs / 1000)
        
        if ($result.Success) {
            $successCount++
            $batchSuccessCount++
            $successfulIds += $jobInfo.Item.id
            
            $fileName = Split-Path $result.OutputFile -Leaf
            Write-Host "  [OK]    " -ForegroundColor Green -NoNewline
            Write-Host "$($result.Id.Substring(0,8))... " -NoNewline
            Write-Host "HTTP:$($result.HttpStatus) " -ForegroundColor DarkGray -NoNewline
            Write-Host "Type:$($result.FileType) " -ForegroundColor DarkYellow -NoNewline
            Write-Host "($elapsed)" -ForegroundColor DarkGray
            Write-Host "          -> $fileName" -ForegroundColor Gray
        }
        else {
            $failedCount++
            $batchFailCount++
            # Decrement sequence counter for failed downloads
            $sequenceCounter--
            
            Write-Host "  [FAIL]  " -ForegroundColor Red -NoNewline
            Write-Host "$($result.Id.Substring(0,8))... " -NoNewline
            Write-Host "HTTP:$($result.HttpStatus) " -ForegroundColor DarkGray -NoNewline
            Write-Host "$($result.Error) " -ForegroundColor Red -NoNewline
            Write-Host "($elapsed)" -ForegroundColor DarkGray
        }
    }
    
    # Remove successful rows from remaining
    if ($successfulIds.Count -gt 0) {
        $successMap = @{}
        foreach ($id in $successfulIds) { $successMap[$id] = $true }
        
        $newRemaining = New-Object System.Collections.ArrayList
        foreach ($row in $remainingRows) {
            if (-not $successMap.ContainsKey($row.id)) {
                $null = $newRemaining.Add($row)
            }
        }
        $remainingRows = $newRemaining
        
        # Save CSV atomically
        $saved = Save-CsvAtomically -CsvPath $InputFile -Rows $remainingRows
        if (-not $saved) {
            Write-Host "  [WARN]  Failed to update CSV file!" -ForegroundColor Yellow
        }
    }
    
    # Batch summary
    Write-Host ""
    Write-Host "  Batch $currentBatch complete: " -NoNewline -ForegroundColor White
    Write-Host "$batchSuccessCount OK" -ForegroundColor Green -NoNewline
    Write-Host ", " -NoNewline
    Write-Host "$batchFailCount failed" -ForegroundColor Red -NoNewline
    Write-Host " | Total: $successCount/$totalRows success, $failedCount failed, $($remainingRows.Count) remaining" -ForegroundColor Gray
    
    # Cooldown if more batches remain
    if ($remainingRows.Count -gt 0 -and $CooldownSeconds -gt 0) {
        Write-Host ""
        for ($i = $CooldownSeconds; $i -gt 0; $i--) {
            Write-Host "`r  Cooldown: $i seconds remaining...   " -ForegroundColor DarkYellow -NoNewline
            Start-Sleep -Seconds 1
        }
        Write-Host "`r  Cooldown complete.                    " -ForegroundColor DarkGray
    }
    
    Write-Host ""
}

$overallStopwatch.Stop()
$totalTime = Format-ElapsedTime -Seconds ($overallStopwatch.ElapsedMilliseconds / 1000)

# Final summary
Write-Host ""
Write-Host ("=" * 75) -ForegroundColor DarkGray
Write-Host "                           DOWNLOAD COMPLETE" -ForegroundColor Green
Write-Host ("=" * 75) -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Total Time:     $totalTime" -ForegroundColor White
Write-Host "  Total Items:    $totalRows" -ForegroundColor White
Write-Host "  Successful:     $successCount" -ForegroundColor Green
Write-Host "  Failed:         $failedCount" -ForegroundColor $(if ($failedCount -gt 0) { "Red" } else { "Green" })
Write-Host "  Remaining:      $($remainingRows.Count)" -ForegroundColor $(if ($remainingRows.Count -gt 0) { "Yellow" } else { "Green" })
Write-Host ""

if ($remainingRows.Count -gt 0) {
    Write-Host "  NOTE: $($remainingRows.Count) items still in CSV - run again to retry." -ForegroundColor Yellow
}

Write-Host "  Output folder:  $OutputDir" -ForegroundColor Cyan
Write-Host ""
