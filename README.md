# Sistema Bancario de Alta Disponibilidad (Raft) - Clúster Distribuido en LAN

Este proyecto implementa una arquitectura distribuida de base de datos bancaria con tolerancia a fallos mediante el algoritmo de consenso Raft (vía etcd). El clúster consta de 5 nodos MySQL distribuidos físicamente en diferentes computadoras dentro de una misma red local (Wi-Fi o Ethernet).

## Arquitectura

- **Red Local (LAN)**: Todos los nodos se comunican de forma directa a través de las IPs locales asignadas por el enrutador Wi-Fi (ej. `192.168.x.x`).
- **Etcd**: Clúster de 3 nodos (Plano de control y bloqueo distribuido).
- **MySQL**: 5 nodos corriendo MySQL 8.0 (1 Maestro y 4 Réplicas dinámicas).
- **Sidecar (Python)**: 5 agentes (1 en cada máquina) conectados a etcd que gestionan el failover y la configuración de replicación de sus respectivos nodos locales.
- **Cliente Bancario (Python)**: App transaccional (ACID) que descubre al maestro actual consultando a etcd y cuenta con reintentos idempotentes.

## CONFIGURACIÓN INICIAL (Obligatoria para todos los compañeros)

1. **Averiguar IP Local**
   - Cada persona debe averiguar su IP local (`192.168.x.x`).
   - *Windows:* Abrir CMD y ejecutar `ipconfig` (buscar Dirección IPv4).
   - *Mac/Linux:* Abrir Terminal y ejecutar `ifconfig` o `ip a`.

2. **Desbloquear Puertos en el Firewall (CRÍTICO)**
   - El Firewall bloqueará la conexión por defecto. Deben abrir los puertos **3306, 2379 y 2380**.
   - **Opción Rápida:** Ejecuten los scripts incluidos en la carpeta `scripts`:
     - *Windows:* Abran PowerShell como Administrador y ejecuten `.\scripts\open-firewall.ps1`.
     - *Mac/Linux:* Abran Terminal y ejecuten `sudo bash scripts/open-firewall.sh`.

3. **Configurar el entorno**
   - Clonar este repositorio.
   - Copiar el archivo `.env.example` y renombrarlo como `.env` en la raíz del proyecto.
   - ¡Trabajen en equipo! Llenen el archivo `.env` con las 5 IPs locales de manera que todos tengan EXACTAMENTE el mismo archivo `.env`.

---

## USO DEL CLÚSTER DISTRIBUIDO

En lugar de levantar todos los contenedores en una sola máquina, cada compañero usará un **perfil** específico.

### Paso 1: Levantar la Infraestructura

**Compañero 1:**
```bash
docker compose --profile node-1 up -d
```

**Compañero 2:**
```bash
docker compose --profile node-2 up -d
```

**Compañero 3:**
```bash
docker compose --profile node-3 up -d
```

**Compañero 4:**
```bash
docker compose --profile node-4 up -d
```

**Compañero 5:**
```bash
docker compose --profile node-5 up -d
```

> **Nota:** Los nodos 1, 2 y 3 corren etcd + MySQL + Sidecar. Los nodos 4 y 5 solo corren MySQL + Sidecar.

### Paso 2: Ejecutar Pruebas (Cualquier Compañero)

Cualquier miembro del equipo puede ejecutar la app bancaria, ya sea de forma automática o interactiva.

#### Opción A: Prueba Automática (Prueba de Oro)
```bash
# Levantar la app de prueba
docker compose --profile app up -d

# Ver las transferencias en vivo
docker compose logs -f banco-app
```
*(Para detenerla, presiona Ctrl+C y luego `docker compose stop banco-app`)*

#### Opción B: Modo Interactivo (Manual)
```bash
docker compose run -it --rm banco-app python src/banco_app.py
```
*(Sigue las instrucciones en pantalla para transferir o consultar saldos).*

### Paso 3: Simular Caída del Maestro (Failover)

El compañero que tenga el **Nodo 1** (que inicia como maestro), debe apagar sus servicios simulando una falla de hardware:
```bash
docker compose stop mysql-node-1 sidecar-1
```
En las pantallas de los demás compañeros (y en la app cliente) verán cómo el clúster elige automáticamente un nuevo maestro en menos de 5 segundos y las transferencias continúan.

### Paso 4: Limpieza
Para apagar y borrar los datos locales al terminar el examen:
```bash
docker compose down -v
```
