# Sistema Bancario de Alta Disponibilidad (Raft) - Clúster Distribuido

Este proyecto implementa una arquitectura distribuida de base de datos bancaria con tolerancia a fallos mediante el algoritmo de consenso Raft (vía etcd). El clúster consta de 5 nodos MySQL distribuidos físicamente en diferentes computadoras mediante una red VPN (Tailscale).

## Arquitectura

- **Red VPN Mesh (Tailscale)**: Todos los nodos se comunican de forma segura a través de IPs globales estáticas `100.x.x.x` que evitan problemas de firewalls locales.
- **Etcd**: Clúster de 3 nodos (Plano de control y bloqueo distribuido).
- **MySQL**: 5 nodos corriendo MySQL 8.0 (1 Maestro y 4 Réplicas dinámicas).
- **Sidecar (Python)**: 5 agentes (1 en cada máquina) conectados a etcd que gestionan el failover y la configuración de replicación de sus respectivos nodos locales.
- **Cliente Bancario (Python)**: App transaccional (ACID) que descubre al maestro actual consultando a etcd y cuenta con reintentos idempotentes.

## CONFIGURACIÓN INICIAL (Obligatoria para todos los compañeros)

1. **Instalar Tailscale**
   - Cada persona debe descargar e instalar [Tailscale](https://tailscale.com/) en su computadora e iniciar sesión.
   - Anotar la IP asignada (la que empieza con `100.x.x.x`).

2. **Configurar el entorno**
   - Clonar este repositorio.
   - Copiar el archivo `.env.example` y renombrarlo como `.env` en la raíz del proyecto.
   - ¡Trabajen en equipo! Llenen el archivo `.env` con las 5 IPs de Tailscale de manera que todos tengan EXACTAMENTE el mismo archivo `.env`.

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
