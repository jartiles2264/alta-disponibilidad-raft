# Archivo: scripts/open-firewall.ps1
# Proposito: Abrir los puertos del Firewall de Windows para el Clúster Raft
# Instrucciones: Ejecutar este script desde PowerShell como Administrador

Write-Host "=======================================================" -ForegroundColor Cyan
Write-Host " Configurando Firewall de Windows para Examen Naranjo  " -ForegroundColor Cyan
Write-Host "=======================================================" -ForegroundColor Cyan
Write-Host ""

# Verificar si el script se esta corriendo como Administrador
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: Este script necesita permisos de Administrador." -ForegroundColor Red
    Write-Host "Por favor, abre PowerShell como Administrador y vuelve a ejecutarlo." -ForegroundColor Yellow
    Exit
}

$ports = @(3306, 2379, 2380)

foreach ($port in $ports) {
    $ruleName = "ExamenNaranjo_Port_$port"
    
    # Comprobar si la regla ya existe
    $existingRule = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
    if ($existingRule) {
        Write-Host "La regla para el puerto $port ya existe. Actualizandola..." -ForegroundColor Yellow
        Remove-NetFirewallRule -DisplayName $ruleName
    }

    Write-Host "Abriendo puerto TCP $port..."
    New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -LocalPort $port -Protocol TCP -Action Allow -Profile Any | Out-Null
}

Write-Host ""
Write-Host "¡Puertos 3306, 2379 y 2380 abiertos exitosamente!" -ForegroundColor Green
Write-Host "Ahora tus companeros deberian poder conectarse a tus contenedores." -ForegroundColor Green
Write-Host "=======================================================" -ForegroundColor Cyan
