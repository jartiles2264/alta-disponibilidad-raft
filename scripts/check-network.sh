#!/bin/bash
# =============================================================
# Script de diagnóstico de conectividad para Mac/Linux
# Ejecutar con: bash scripts/check-network.sh
# =============================================================

PEERS=("$@")

if [ ${#PEERS[@]} -eq 0 ]; then
  echo "Uso: bash scripts/check-network.sh IP1 IP2 IP3 ..."
  echo "Ejemplo: bash scripts/check-network.sh 172.20.10.2 172.20.10.3 172.20.10.4"
  exit 1
fi

MY_IP=$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || echo "DESCONOCIDA")
echo "=================================================="
echo " DIAGNOSTICO DE RED - NODO: $MY_IP"
echo "=================================================="
echo ""

PORTS=(2379 2380 3306)

for peer in "${PEERS[@]}"; do
  if [ "$peer" = "$MY_IP" ] || [ "$peer" = "127.0.0.1" ] || [ "$peer" = "127.0.0.2" ]; then
    continue
  fi
  echo "--- Comprobando nodo: $peer ---"
  for port in "${PORTS[@]}"; do
    result=$(nc -z -w 2 "$peer" "$port" 2>&1)
    if [ $? -eq 0 ]; then
      echo "  [OK]  Puerto $port en $peer: ABIERTO"
    else
      echo "  [FAIL] Puerto $port en $peer: BLOQUEADO o INACCESIBLE"
    fi
  done
  echo ""
done

echo "--- Comprobando puertos locales (los que exponemos) ---"
for port in "${PORTS[@]}"; do
  if lsof -i ":$port" -sTCP:LISTEN -n -P > /dev/null 2>&1; then
    echo "  [OK]  Puerto $port local: ESCUCHANDO"
  else
    echo "  [WARN] Puerto $port local: NO escuchando (contenedor no levantado?)"
  fi
done
echo ""
echo "MI IP: $MY_IP"
echo "=================================================="
