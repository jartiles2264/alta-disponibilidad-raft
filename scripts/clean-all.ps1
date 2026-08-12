<#
.SYNOPSIS
LIMPIEZA NUCLEAR - Elimina TODOS los contenedores y volúmenes del proyecto Examen Naranjo.
Ejecutar ANTES de docker compose up en cada nueva sesión.
#>

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host " LIMPIEZA TOTAL del proyecto Examen Naranjo" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# 1. Detener y eliminar con compose
Write-Host "Deteniendo contenedores del compose actual..."
docker compose down -v 2>$null

# 2. Forzar eliminación de contenedores zombies por nombre
$containers = @("sidecar-1", "sidecar-2", "sidecar-3", "sidecar-4", "sidecar-5",
                "etcd-1", "etcd-2", "etcd-3",
                "mysql-node-1", "mysql-node-2", "mysql-node-3", "mysql-node-4", "mysql-node-5",
                "mysql-node4", "banco-app")

foreach ($c in $containers) {
    docker rm -f $c 2>$null
    if ($?) { Write-Host "  Eliminado contenedor: $c" -ForegroundColor Green }
}

# 3. Eliminar volúmenes del proyecto 
# (Filtramos por "examennaranjo" o "alta-disponibilidad" según el nombre del folder)
$volumes = docker volume ls --format '{{.Name}}' | Select-String -Pattern "(examennaranjo|alta-disponibilidad)" | ForEach-Object { $_.Line }
foreach ($v in $volumes) {
    docker volume rm $v 2>$null
    if ($?) { Write-Host "  Eliminado volumen: $v" -ForegroundColor Green }
}

Write-Host "`nEstado final - Contenedores:" -ForegroundColor Yellow
docker ps -a --format "  {{.Names}}: {{.Status}}"

Write-Host "`n¡Limpieza completa! Ya puedes levantar tu nodo:" -ForegroundColor Cyan
Write-Host "  docker compose --profile node-X up -d --build" -ForegroundColor White
Write-Host "==================================================" -ForegroundColor Cyan
