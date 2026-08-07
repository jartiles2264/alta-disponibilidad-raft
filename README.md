# Sistema Bancario de Alta Disponibilidad (Raft)

Este proyecto implementa una arquitectura distribuida de base de datos bancaria con tolerancia a fallos mediante el algoritmo de consenso Raft (via etcd). El cluster consta de 5 nodos MySQL con un proceso Sidecar acoplado a cada uno, los cuales negocian dinamicamente el liderazgo. Ante una caida catastrofica del nodo principal, los nodos restantes eligen atomicamente a un nuevo maestro y auto-reconfiguran la replicacion sin perdida de datos.

## Arquitectura
- Etcd: Cluster de 3 nodos (Plano de control y bloqueo distribuido).
- MySQL: 5 nodos corriendo MySQL 8.0 (1 Maestro y 4 Replicas dinamicas).
- Sidecar (Python): 5 agentes conectados a etcd que gestionan el failover y la configuracion de replicacion de sus respectivos nodos locales.
- Cliente Bancario (Python): App transaccional (ACID) que descubre al maestro actual consultando a etcd y cuenta con reintentos idempotentes.

## ESCENARIO 1: Ejecutar la "Prueba de Oro" (Automatica)

Levantar todo y empezar la prueba:
```bash
docker compose up -d
```

Ver como se hacen las transferencias en vivo:
```bash
docker compose logs -f banco-app
```
(Presiona Ctrl + C para dejar de mirar).

Simular caida del Maestro (para probar failover):
(En otra terminal)
```bash
docker compose stop mysql-node-1 sidecar-1
```

Apagar y borrar todo al terminar (incluyendo bases de datos):
```bash
docker compose down -v
```

## ESCENARIO 2: Ejecutar de manera "Interactiva" (Manual)

Levantar el cluster pero con la prueba automatica apagada:
```bash
docker compose up -d --scale banco-app=0
```

Entrar al Menu Interactivo en tu terminal:
```bash
docker compose run -it --rm banco-app python src/banco_app.py
```
(Sigue las instrucciones en pantalla para transferir o consultar saldos. Al terminar elige Salir).

Apagar y borrar todo al terminar:
```bash
docker compose down -v
```
