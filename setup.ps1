# ============================================================================
# Post-template setup script for ai-data-registry (Windows PowerShell)
# Run this after creating a new repo from the GitHub template.
# It replaces placeholder values and reinitializes the project for your use.
# ============================================================================

#Requires -Version 7.0
$ErrorActionPreference = 'Stop'

Write-Host "`nai-data-registry template setup`n" -ForegroundColor Cyan

# --- Check pixi is installed -----------------------------------------------

if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
    Write-Host 'pixi is not installed.' -ForegroundColor Red
    Write-Host ''
    Write-Host 'Install pixi first:'
    Write-Host '  winget install prefix-dev.pixi                              # Windows (winget)'
    Write-Host '  iwr -useb https://pixi.sh/install.ps1 | iex                # Windows (PowerShell)'
    Write-Host '  curl -fsSL https://pixi.sh/install.sh | bash               # macOS / Linux'
    Write-Host '  brew install pixi                                           # macOS (Homebrew)'
    Write-Host ''
    Write-Host 'Then re-run: .\setup.ps1'
    exit 1
}

Write-Host "  pixi found: $(pixi --version)" -ForegroundColor Green
Write-Host ''

# --- Gather info -----------------------------------------------------------

$ProjectName = Read-Host 'Project name (e.g. my-geo-project)'
if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    Write-Host 'Project name is required.' -ForegroundColor Red
    exit 1
}

$DefaultAuthor = try { git config user.name } catch { 'Your Name' }
$AuthorName = Read-Host "Author name [$DefaultAuthor]"
if ([string]::IsNullOrWhiteSpace($AuthorName)) { $AuthorName = $DefaultAuthor }

$DefaultEmail = try { git config user.email } catch { 'you@example.com' }
$AuthorEmail = Read-Host "Author email [$DefaultEmail]"
if ([string]::IsNullOrWhiteSpace($AuthorEmail)) { $AuthorEmail = $DefaultEmail }

$Description = Read-Host 'Description (one line) [Geospatial data processing project]'
if ([string]::IsNullOrWhiteSpace($Description)) { $Description = 'Geospatial data processing project' }

$Version = Read-Host 'Version [0.1.0]'
if ([string]::IsNullOrWhiteSpace($Version)) { $Version = '0.1.0' }

Write-Host "`nApplying settings..." -ForegroundColor Yellow

# --- Replace placeholders in pixi.toml ------------------------------------

$pixiToml = Get-Content -Path 'pixi.toml' -Raw
$pixiToml = $pixiToml -replace 'name = "ai-data-registry"', "name = `"$ProjectName`""
$pixiToml = $pixiToml -replace 'authors = \[.*?\]', "authors = [`"$AuthorName <$AuthorEmail>`"]"
$pixiToml = $pixiToml -replace 'version = "0.1.0"', "version = `"$Version`""
Set-Content -Path 'pixi.toml' -Value $pixiToml -NoNewline

# --- Replace placeholders in CLAUDE.md ------------------------------------

$claudeMd = Get-Content -Path 'CLAUDE.md' -Raw
$claudeMd = $claudeMd -replace 'ai-data-registry', $ProjectName
Set-Content -Path 'CLAUDE.md' -Value $claudeMd -NoNewline

# --- Replace in .claude/ agent/skill files --------------------------------

Get-ChildItem -Path '.claude' -Filter '*.md' -Recurse | ForEach-Object {
    $content = Get-Content -Path $_.FullName -Raw
    if ($content -match 'ai-data-registry') {
        $content = $content -replace 'ai-data-registry', $ProjectName
        Set-Content -Path $_.FullName -Value $content -NoNewline
    }
}

# --- Clean up template-specific files --------------------------------------

Remove-Item -Path '.github/workflows/template-setup.yml' -ErrorAction SilentlyContinue

# --- Install pixi environment ---------------------------------------------

Write-Host 'Running pixi install...' -ForegroundColor Yellow
pixi install

# --- Remove setup scripts (after everything succeeds) ----------------------

Remove-Item -Path 'setup.sh' -ErrorAction SilentlyContinue
Remove-Item -Path 'setup.ps1' -ErrorAction SilentlyContinue

# --- Done ------------------------------------------------------------------

Write-Host "`nDone! Project '$ProjectName' is ready.`n" -ForegroundColor Green
Write-Host 'Next steps:'
Write-Host "  1. Review pixi.toml and CLAUDE.md"
Write-Host '  2. Run:  pixi install'
Write-Host '  3. Create your first workspace:  /project:new-workspace <name> <language>'
Write-Host "  4. Commit:  git add -A && git commit -m 'Initialize $ProjectName from template'"
Write-Host ''
