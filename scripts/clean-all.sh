#!/bin/bash
# =============================================================
# LIMPIEZA NUCLEAR — Elimina TODOS los contenedores y volúmenes
# del proyecto Examen Naranjo, incluyendo huerfanos/zombies.
# Ejecutar ANTES de docker compose up en cada nueva sesión.
#
# Mac/Linux: bash scripts/clean-all.sh
# =============================================================

echo "=================================================="
echo " LIMPIEZA TOTAL del proyecto Examen Naranjo"
echo "=================================================="

# 1. Detener y eliminar con compose
docker compose down -v 2>/dev/null

# 2. Forzar eliminación de contenedores por nombre (zombies)
CONTAINERS=(
  sidecar-1 sidecar-2 sidecar-3 sidecar-4 sidecar-5
  etcd-1 etcd-2 etcd-3
  mysql-node-1 mysql-node-2 mysql-node-3 mysql-node-4 mysql-node-5
  mysql-node4
  banco-app
)

for c in "${CONTAINERS[@]}"; do
  docker rm -f "$c" 2>/dev/null && echo "  Eliminado contenedor: $c"
done

# 3. Eliminar volúmenes del proyecto
VOLUMES=$(docker volume ls --format '{{.Name}}' | grep -i "examennaranjo")
for v in $VOLUMES; do
  docker volume rm "$v" 2>/dev/null && echo "  Eliminado volumen: $v"
done

echo ""
echo "Estado final — Contenedores:"
docker ps -a --format "  {{.Names}}: {{.Status}}"

echo ""
echo "¡Limpieza completa! Ya puedes levantar tu nodo:"
echo "  docker compose --profile node-X up -d --build"
echo "=================================================="
