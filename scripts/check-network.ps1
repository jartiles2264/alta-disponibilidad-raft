# =============================================================
# Script de diagnóstico de conectividad para Windows (PowerShell)
# Ejecutar como Administrador:
#   .\scripts\check-network.ps1 -Peers 172.20.10.2,172.20.10.3,172.20.10.4
# =============================================================

param(
    [Parameter(Mandatory=$true)]
    [string[]]$Peers
)

$MyIP = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
    $_.InterfaceAlias -notmatch "Loopback" -and
    $_.InterfaceAlias -notmatch "Hyper-V" -and
    $_.InterfaceAlias -notmatch "vEthernet" -and
    $_.InterfaceAlias -notmatch "WSL" -and
    $_.IPAddress -notmatch "^169\." -and
    $_.IPAddress -notmatch "^127\."
} | Select-Object -First 1).IPAddress

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host " DIAGNOSTICO DE RED - NODO: $MyIP" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

$ports = @(2379, 2380, 3306)

foreach ($peer in $Peers) {
    if ($peer -eq $MyIP -or $peer -eq "127.0.0.1" -or $peer -eq "127.0.0.2") {
        continue
    }
    Write-Host "--- Comprobando nodo: $peer ---" -ForegroundColor Yellow
    foreach ($port in $ports) {
        $result = Test-NetConnection -ComputerName $peer -Port $port -WarningAction SilentlyContinue
        if ($result.TcpTestSucceeded) {
            Write-Host "  [OK]  Puerto $port en $peer : ABIERTO" -ForegroundColor Green
        } else {
            Write-Host "  [FAIL] Puerto $port en $peer : BLOQUEADO o INACCESIBLE" -ForegroundColor Red
        }
    }
    Write-Host ""
}

Write-Host "--- Comprobando puertos locales (los que exponemos) ---" -ForegroundColor Yellow
foreach ($port in $ports) {
    $listening = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    if ($listening) {
        Write-Host "  [OK]  Puerto $port local: ESCUCHANDO" -ForegroundColor Green
    } else {
        Write-Host "  [WARN] Puerto $port local: NO escuchando (contenedor no levantado?)" -ForegroundColor DarkYellow
    }
}

Write-Host ""
Write-Host "MI IP: $MyIP" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
