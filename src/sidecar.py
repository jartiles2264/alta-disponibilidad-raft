"""
Agente sidecar de failover automatico.

Adaptado para integrarse perfectamente en el proyecto.
"""

import os
import random
import signal
import sys
import time
from datetime import datetime

# Importaciones locales
from etcd_client import EtcdClient, EtcdError
from mysql_ops import MySQLOps

CLAVE_MAESTRO = os.environ.get("ETCD_KEY", "/banco/mysql/primary")
PREFIJO_NODOS = "/banco/nodos/"


def log(mensaje):
    print(f"{datetime.now():%H:%M:%S}  {mensaje}", flush=True)


def env(nombre, defecto=None):
    valor = os.environ.get(nombre, defecto)
    if valor is None:
        raise SystemExit(f"Falta la variable de entorno {nombre}")
    return valor


class AgenteFailover:
    def __init__(self):
        # Determinar NODE_IP
        self.node_ip = env("NODE_IP")
        
        # Determinar NODE_ID de forma dinamica si no viene en el entorno
        self.node_id = os.environ.get("NODE_ID")
        if not self.node_id:
            mysql_host = os.environ.get("MYSQL_HOST", "")
            if "node-" in mysql_host:
                self.node_id = mysql_host.split("node-")[-1]
            else:
                nodes_raw = os.environ.get("MYSQL_NODES", "")
                nodes = [n.strip() for n in nodes_raw.split(",") if n.strip()]
                if self.node_ip in nodes:
                    self.node_id = str(nodes.index(self.node_ip) + 1)
                else:
                    self.node_id = "1"

        self.ttl = int(env("LEASE_TTL", "6"))
        self.puerto_mysql = int(env("MYSQL_PORT", "3306"))

        # etcd client
        self.etcd = EtcdClient(env("ETCD_ENDPOINTS"))

        # Determinar credenciales administrativas para MySQL local
        root_pass = os.environ.get("MYSQL_ROOT_PASSWORD")
        if root_pass:
            mysql_user = "root"
            mysql_pass = root_pass
        else:
            mysql_user = env("SIDECAR_MYSQL_USER", "sidecar")
            mysql_pass = env("SIDECAR_MYSQL_PASSWORD")

        # Operaciones MySQL
        self.mysql = MySQLOps(
            host=env("MYSQL_HOST", "mysql"),
            puerto=self.puerto_mysql,
            usuario=mysql_user,
            password=mysql_pass,
            repl_usuario=env("MYSQL_REPL_USER", "replica_user"),
            repl_password=env("MYSQL_REPL_PASSWORD", "replicapass123"),
            app_usuario=env("MYSQL_USER", "banco_app"),
            app_password=env("MYSQL_PASSWORD", "bancopass123"),
        )

        self.soy_maestro = False
        self.lease = None
        self.lease_estado = None
        self.ultimo_latido_ok = 0.0
        self.fuente_configurada = None
        self.fallos_seguidos = 0
        self.corriendo = True
        self.ciclos = 0

    # ------------------------------------------------------------------
    # Ciclo principal
    # ------------------------------------------------------------------
    def arrancar(self):
        log(f"Agente del nodo {self.node_id} ({self.node_ip}) iniciado.")
        log(f"etcd: {', '.join(self.etcd.endpoints)}")
        self.esperar_mysql()

        while self.corriendo:
            try:
                self.ciclo()
            except EtcdError as exc:
                log(f"[etcd] sin contacto: {exc}")
                self.vigilar_fencing()
            except Exception as exc:  # noqa: BLE001
                log(f"[error] {type(exc).__name__}: {exc}")
            time.sleep(1)

    def esperar_mysql(self):
        log("Esperando a que MySQL local acepte conexiones...")
        while not self.mysql.esta_vivo():
            time.sleep(2)
        log("MySQL local responde.")

    def ciclo(self):
        if not self.mysql.esta_vivo():
            self.fallos_seguidos += 1
            if self.fallos_seguidos == 3:
                log("MySQL local no responde (3 intentos).")
                if self.soy_maestro:
                    self.renunciar()
            return
        self.fallos_seguidos = 0
        self.ciclos += 1

        if self.ciclos % 5 == 1:
            self.publicar_estado()

        valor, _ = self.etcd.get_con_lease(CLAVE_MAESTRO)

        if valor == self.node_ip:
            self.actuar_como_maestro()
        elif valor is not None:
            self.actuar_como_replica(valor)
        else:
            self.competir_por_liderazgo()

    # ------------------------------------------------------------------
    # Rol: maestro
    # ------------------------------------------------------------------
    def actuar_como_maestro(self):
        if self.lease is None:
            log("La clave del maestro ya apunta a este nodo; readoptando.")
            self.lease = self.etcd.lease_grant(self.ttl)
            self.etcd.put(CLAVE_MAESTRO, self.node_ip, self.lease)
            self.soy_maestro = True
            self.ultimo_latido_ok = time.time()
            self.fuente_configurada = None
            if not self.mysql.es_escribible():
                self.mysql.promover_a_maestro()
            try:
                self.mysql.asegurar_usuarios()
            except Exception as exc:
                log(f"No se pudieron asegurar los usuarios: {exc}")
            return

        if not self.soy_maestro:
            self.soy_maestro = True

        ttl_restante = self.etcd.lease_keepalive(self.lease)
        if ttl_restante <= 0:
            log("El lease expiro: se pierde el liderazgo.")
            self.soy_maestro = False
            self.lease = None
            self.mysql.solo_lectura()
            return

        self.ultimo_latido_ok = time.time()
        if not self.mysql.es_escribible():
            log("Reafirmando modo escritura en el MySQL local.")
            self.mysql.promover_a_maestro()

    def renunciar(self):
        log("Renunciando al liderazgo (MySQL local caido).")
        if self.lease:
            self.etcd.lease_revoke(self.lease)
        self.soy_maestro = False
        self.lease = None

    def vigilar_fencing(self):
        if not self.soy_maestro:
            return
        if time.time() - self.ultimo_latido_ok > self.ttl:
            log("Sin renovar el lease por mas de un TTL: me degrado a solo lectura para no provocar split-brain.")
            try:
                self.mysql.solo_lectura()
            except Exception as exc:  # noqa: BLE001
                log(f"No se pudo aplicar solo lectura: {exc}")
            self.soy_maestro = False
            self.lease = None

    # ------------------------------------------------------------------
    # Rol: replica
    # ------------------------------------------------------------------
    def actuar_como_replica(self, ip_maestro):
        if self.soy_maestro:
            log(f"Otro nodo ({ip_maestro}) es el maestro; cediendo el paso.")
            self.soy_maestro = False
            self.lease = None

        if self.fuente_configurada != ip_maestro or self.mysql.fuente_actual() != ip_maestro:
            log(f"Configurando replicacion desde {ip_maestro}.")
            self.mysql.degradar_a_replica(ip_maestro, self.puerto_mysql)
            self.fuente_configurada = ip_maestro
            return

        error = self.mysql.replicacion_rota()
        if error and ("1236" in error or "GTID" in error.upper()):
            log(f"Historial GTID divergente: {error[:120]}")
            log("Reincorporando este nodo desde cero contra el nuevo maestro.")
            self.mysql.resync_desde_cero(ip_maestro, self.puerto_mysql)

    # ------------------------------------------------------------------
    # Eleccion
    # ------------------------------------------------------------------
    def competir_por_liderazgo(self):
        if self.soy_maestro:
            self.soy_maestro = False
            self.lease = None
            self.mysql.solo_lectura()

        espera = self.retardo_por_atraso()
        if espera > 0:
            log(f"No hay maestro. Hay nodos mas al dia; espero {espera:.1f}s.")
            time.sleep(espera)
            if self.etcd.get(CLAVE_MAESTRO) is not None:
                return  # alguien mejor ya gano

        self.aplicar_relay_log_pendiente()

        lease = self.etcd.lease_grant(self.ttl)
        if self.etcd.crear_si_no_existe(CLAVE_MAESTRO, self.node_ip, lease):
            self.lease = lease
            self.soy_maestro = True
            self.ultimo_latido_ok = time.time()
            self.fuente_configurada = None
            log("=" * 58)
            log(f"  GANE LA ELECCION -> este nodo ({self.node_ip}) es MAESTRO")
            log("=" * 58)
            self.mysql.promover_a_maestro()
            try:
                self.mysql.asegurar_usuarios()
            except Exception as exc:
                log(f"No se pudieron asegurar los usuarios: {exc}")
        else:
            self.etcd.lease_revoke(lease)

    def retardo_por_atraso(self):
        try:
            mias = self.mysql.transacciones_aplicadas()
        except Exception:  # noqa: BLE001
            return 0.0

        adelantados = 0
        for clave, valor in self.etcd.get_prefijo(PREFIJO_NODOS).items():
            if clave.endswith(self.node_id):
                continue
            try:
                _, conteo = valor.split(";")
                if int(conteo) > mias:
                    adelantados += 1
            except ValueError:
                continue
        if adelantados == 0:
            return 0.0
        return adelantados * 0.6 + random.uniform(0, 0.2)

    def aplicar_relay_log_pendiente(self, limite=3.0):
        fin = time.time() + limite
        while time.time() < fin:
            try:
                if not self.mysql.relay_log_pendiente():
                    return
            except Exception:  # noqa: BLE001
                return
            log("Aplicando transacciones pendientes del relay log...")
            time.sleep(0.3)

    # ------------------------------------------------------------------
    # Estado publicado
    # ------------------------------------------------------------------
    def publicar_estado(self):
        try:
            conteo = self.mysql.transacciones_aplicadas()
        except Exception:  # noqa: BLE001
            return

        if self.lease_estado is not None:
            if self.etcd.lease_keepalive(self.lease_estado) > 0:
                self.etcd.put(
                    PREFIJO_NODOS + self.node_id,
                    f"{self.node_ip};{conteo}",
                    self.lease_estado,
                )
                return
            self.lease_estado = None

        self.lease_estado = self.etcd.lease_grant(self.ttl * 3)
        self.etcd.put(
            PREFIJO_NODOS + self.node_id,
            f"{self.node_ip};{conteo}",
            self.lease_estado,
        )

    def detener(self, *_):
        log("Deteniendo el agente...")
        self.corriendo = False
        if self.soy_maestro and self.lease:
            self.etcd.lease_revoke(self.lease)
        sys.exit(0)


if __name__ == "__main__":
    agente = AgenteFailover()
    signal.signal(signal.SIGTERM, agente.detener)
    signal.signal(signal.SIGINT, agente.detener)
    agente.arrancar()
