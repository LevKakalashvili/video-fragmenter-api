# === Настрой исключаемые каталоги ===
$excludeDirs = @(
    '.git',
    '.gitlab-ci.yml',
    '.gitignore',
    '.idea',
    'logs',
    'data',
    '.venv',
    'venv',
    '_CI_CD',
    '__pycache__',
    '.pytest_cache',
    'prepare_folder_ci_cd.ps1',
    'prepare_folder_chatgpt.ps1',
    'poetry.lock'
)

# Получить имя текущей директории
$currentDir = Split-Path -Leaf (Get-Location)
$targetDir = "$currentDir"+"_chatgpt"

# Динамически добавить целевую папку к исключениям (если она вдруг отличается по имени)
if ($excludeDirs -notcontains $targetDir) {
    $excludeDirs += $targetDir
}

# === Укажи исключаемые файлы (по точному имени) ===
$excludeFiles = @(
    '.gitignore',
    '.env',
    '.*_CI_CD'
)

# === Укажи маски файлов для исключения (wildcards) ===
$excludeFilePatterns = @(
    '*.pyc',
    '*.yaml',
    '*.yml',
    '*.log',
    '*.tmp',
    '*.ps1',
    '*.zip'
)

# === Максимальный размер файла для включения в архив ===
$maxFileSizeBytes = 5MB

# Создать целевой каталог, если его нет
if (-not (Test-Path $targetDir)) {
    New-Item -ItemType Directory -Path $targetDir | Out-Null
}

function Copy-With-Exclude {
    param (
        [string]$source,
        [string]$dest,
        [string[]]$excludeDirs,
        [string[]]$excludeFiles,
        [string[]]$excludeFilePatterns,
        [long]$maxFileSizeBytes
    )
    Get-ChildItem -Path $source -Force | ForEach-Object {
        # Исключение директорий по имени
        if ($_.PSIsContainer -and ($excludeDirs -contains $_.Name)) {
            return
        }
        # Исключение файлов по точному имени
        if (-not $_.PSIsContainer -and ($excludeFiles -contains $_.Name)) {
            return
        }
        # Исключение файлов по маске
        if (-not $_.PSIsContainer) {
            foreach ($pattern in $excludeFilePatterns) {
                if ($_.Name -like $pattern) { return }
            }
        }
        # Исключение файлов больше 5 МБ
        if (-not $_.PSIsContainer -and $_.Length -gt $maxFileSizeBytes) {
            return
        }
        $targetPath = Join-Path $dest $_.Name
        if ($_.PSIsContainer) {
            New-Item -ItemType Directory -Path $targetPath -Force | Out-Null
            Copy-With-Exclude -source $_.FullName -dest $targetPath -excludeDirs $excludeDirs -excludeFiles $excludeFiles -excludeFilePatterns $excludeFilePatterns -maxFileSizeBytes $maxFileSizeBytes
        } else {
            Copy-Item -Path $_.FullName -Destination $targetPath -Force
        }
    }
}

Copy-With-Exclude -source "." -dest $targetDir -excludeDirs $excludeDirs -excludeFiles $excludeFiles -excludeFilePatterns $excludeFilePatterns -maxFileSizeBytes $maxFileSizeBytes

# === Создание архива ===
$zipName = "$($targetDir).zip"
$zipPath = Join-Path (Get-Location) $zipName

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

try {
    Compress-Archive -Path (Join-Path $targetDir '*') -DestinationPath $zipPath -Force -ErrorAction Stop

    if (Test-Path $zipPath) {
        Write-Host "Архив создан: $zipPath"

        # Удаляем каталог, из которого собирался архив
        Remove-Item -Path $targetDir -Recurse -Force -ErrorAction Stop
        Write-Host "Каталог удалён: $targetDir"
    }
}
catch {
    Write-Host "Ошибка при создании архива: $($_.Exception.Message)"
}

# === Открыть проводник в текущей папке ===
Invoke-Item (Get-Location)
