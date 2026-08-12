# Sistema Bancario de Alta Disponibilidad — Cluster Distribuido en LAN

Cluster de 5 nodos MySQL con eleccion de maestro via algoritmo Raft (etcd), distribuido en maquinas fisicas conectadas a la misma red Wi-Fi o Hotspot.

## Arquitectura

- **5 nodos MySQL** — 1 Maestro activo + 4 Replicas sincronicas
- **3 nodos etcd** — Quorum Raft para eleccion de lider (tolera 1 caida)
- **5 Sidecars Python** — Agentes de coordinacion, 1 por maquina
- **Red local directa** — Los contenedores usan `network_mode: host`, por lo que se comunican directamente a traves de la red Wi-Fi/Hotspot sin intermediarios Docker

---

## CONFIGURACION INICIAL (Hacerlo una sola vez)

### Paso 1 — Cada quien averigua su IP local

**Mac/Linux:**
```bash
ipconfig getifaddr en0
```

**Windows (PowerShell):**
```powershell
ipconfig
# Buscar "Direccion IPv4" — ignorar IPs que empiecen con 169.x
```

### Paso 2 — Crear el archivo .env (Solo el lider, luego compartirlo)

Copiar `.env.example` a `.env` y llenar con las IPs reales:

```env
LAN_IP_1=172.20.10.2   # IP del Nodo 1 (Maestro inicial)
LAN_IP_2=172.20.10.3   # IP del Nodo 2
LAN_IP_3=172.20.10.4   # IP del Nodo 3
LAN_IP_4=172.20.10.5   # IP del Nodo 4 (o 127.0.0.1 si no hay)
LAN_IP_5=127.0.0.2     # IP del Nodo 5 (o 127.0.0.2 si no hay)

MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_PASSWORD=bancopass123
SIDECAR_MYSQL_PASSWORD=sidecarpass123
MYSQL_REPL_PASSWORD=replicapass123
```

**CRITICO:** Pasar este archivo `.env` a todos los compañeros. Deben tener exactamente el mismo.

### Paso 3 — Desbloquear Firewall (Solo Windows)

Abrir PowerShell **como Administrador:**
```powershell
.\scripts\open-firewall.ps1
```

Ademas, cambiar la red Wi-Fi a **"Privada"** en Configuracion de Windows.

---

## USO — Levantar el cluster

### Limpieza obligatoria antes de cada sesion nueva

```bash
docker compose down -v
```

### Levantar el nodo propio

Cada quien ejecuta su perfil con el flag `--build` (obligatorio para que Docker lea el `.env` actualizado):

```bash
# Nodo 1:
docker compose --profile node-1 up -d --build

# Nodo 2:
docker compose --profile node-2 up -d --build

# Nodo 3:
docker compose --profile node-3 up -d --build

# Nodo 4:
docker compose --profile node-4 up -d --build
```

### Ver los logs en vivo

```bash
docker compose logs -f sidecar-1   # cambiar el numero segun el nodo
```

El cluster esta listo cuando se ve: `PROMOTION complete` en un nodo y `REPLICA configured` en los demas.

---

## Pruebas

### Prueba de Oro (automatica)

```bash
docker compose --profile app up -d --build
docker compose logs -f banco-app
```

### Modo interactivo (manual)

```bash
docker compose run -it --rm banco-app python src/banco_app.py
```

### Simular caida del Maestro (Failover)

En la maquina del Nodo 1:
```bash
docker compose stop mysql-node-1 sidecar-1
```

Observar en los logs de los otros nodos como el sistema elige automaticamente un nuevo Maestro.

---

## Diagnostico de conectividad

**Mac/Linux:**
```bash
bash scripts/check-network.sh 172.20.10.2 172.20.10.3 172.20.10.4
```

**Windows (PowerShell como Admin):**
```powershell
.\scripts\check-network.ps1 -Peers 172.20.10.2,172.20.10.3,172.20.10.4
```

---

## Apagar todo

```bash
docker compose down -v
```

---

## Cambiar de red Wi-Fi

1. Todos averiguan su nueva IP
2. El lider actualiza `.env` con nuevas IPs y lo comparte
3. Todos ejecutan `docker compose down -v`
4. Todos ejecutan `docker compose --profile node-X up -d --build`

El `--build` es esencial para que Docker tome las nuevas IPs.
