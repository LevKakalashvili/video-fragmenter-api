$ffmpegBin = Join-Path $PSScriptRoot "..\tools\ffmpeg\bin"
$ffmpegBin = [System.IO.Path]::GetFullPath($ffmpegBin)

if (-not (Test-Path $ffmpegBin)) {
    Write-Warning "FFmpeg folder not found: $ffmpegBin"
    Write-Warning "Expected path: tools\\ffmpeg\\bin\\ffmpeg.exe"
    return
}

$ffmpegExe = Join-Path $ffmpegBin "ffmpeg.exe"
if (-not (Test-Path $ffmpegExe)) {
    Write-Warning "FFmpeg executable not found: $ffmpegExe"
    return
}

$ffmpegSize = (Get-Item $ffmpegExe).Length
if ($ffmpegSize -lt 5000000) {
    Write-Warning "ffmpeg.exe looks invalid (too small: $ffmpegSize bytes)."
    Write-Warning "Download a full FFmpeg build and replace files in tools\\ffmpeg\\bin."
    return
}

$env:PATH = "$ffmpegBin;$env:PATH"

Write-Host "Added to PATH: $ffmpegBin"
Write-Host "ffmpeg version:"
ffmpeg -version 2>&1 | Select-Object -First 1
