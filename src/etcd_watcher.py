"""
etcd_watcher.py — Observador de etcd con multi-endpoint
=======================================================

Hilo daemon que vigila la clave ``/banco/mysql/primary`` en el
clúster etcd.  Cuando detecta un cambio de IP (evento PUT) o la
eliminación de la clave (evento DELETE por expiración del lease),
ordena al DynamicDatabasePool reconstruirse en caliente.

Mejoras sobre el código guía del examen:
  - Multi-endpoint: recibe una lista de hosts etcd y los recorre
    con round-robin si uno falla (elimina punto único de falla).
  - Backoff exponencial en reconexiones.
  - Señaliza pool_ready para desbloquear al hilo principal.
  - Logging estructurado con timestamps.

Autor: Persona 4
Proyecto: Examen Tercer Parcial — Sistemas Distribuidos
"""

import ipaddress
import os
import logging
import threading
import time

import etcd3

from db_pool import DynamicDatabasePool

# ── Logging ──────────────────────────────────────────────────

logger = logging.getLogger("etcd_watcher")

# ── Constantes ───────────────────────────────────────────────

def _load_default_endpoints():
    """Build default endpoints from cluster-nodes.conf if available."""
    conf_paths = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cluster-nodes.conf"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "cluster-nodes.conf"),
    ]
    for path in conf_paths:
        try:
            conf = {}
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    conf[key.strip()] = value.strip()
            ips = [conf[f"NODE{i}_IP"] for i in range(1, 6) if conf.get(f"NODE{i}_IP")]
            port = conf.get("ETCD_CLIENT_PORT", "2379")
            return ",".join(f"{ip}:{port}" for ip in ips)
        except (OSError, KeyError):
            continue
    return "127.0.0.1:2379"  # Fallback to localhost

_MAX_BACKOFF = 30  # Segundos máximos de espera entre reintentos


def _parse_endpoints(raw):
    """Convierte una cadena 'host:port,host:port,...' en una lista
    de tuplas (host, port).

    >>> _parse_endpoints("10.0.0.1:2379,10.0.0.2:2379")
    [('10.0.0.1', 2379), ('10.0.0.2', 2379)]
    """
    endpoints = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            host, port = entry.rsplit(":", 1)
            port = int(port)
            if not 1 <= port <= 65535:
                raise ValueError(f"Puerto etcd fuera de rango: {port}")
            endpoints.append((host, port))
        else:
            endpoints.append((entry, 2379))
    if not endpoints:
        raise ValueError("ETCD_ENDPOINTS no contiene endpoints")
    return endpoints


def _decode_master_ip(value):
    """Decodifica y valida el valor publicado en etcd.
    Acepta tanto IPs como hostnames."""
    master_addr = value.decode("utf-8").strip()
    if not master_addr:
        raise ValueError("Empty primary address in etcd")
    return master_addr


class EtcdWatcherThread(threading.Thread):
    """Hilo daemon que observa la clave del maestro en etcd.

    Parámetros de configuración leídos del entorno:
      - ETCD_ENDPOINTS: lista de endpoints separados por comas.
      - ETCD_KEY: clave a observar (default /banco/mysql/primary).

    El hilo intenta conectarse a cada endpoint en orden; si uno
    falla, pasa al siguiente (round-robin).  Si todos fallan,
    espera con backoff exponencial y reintenta.
    """

    def __init__(self, etcd_endpoints=None, etcd_key=None):
        super().__init__(name="EtcdWatcher", daemon=True)
        raw = etcd_endpoints or os.getenv("ETCD_ENDPOINTS", _load_default_endpoints())
        self.endpoints = _parse_endpoints(raw)
        self.key = etcd_key or os.getenv("ETCD_KEY", "/banco/mysql/primary")
        self.pool_mgr = DynamicDatabasePool.get_instance()
        self._stop_event = threading.Event()

    # ── Ciclo principal ──────────────────────────────────────

    def run(self):
        logger.info("Hilo de observación etcd iniciado.")
        logger.info(
            "Endpoints configurados: %s",
            ", ".join(f"{h}:{p}" for h, p in self.endpoints),
        )

        backoff = 1

        while not self._stop_event.is_set():
            client = self._connect()
            if client is None:
                # Todos los endpoints fallaron
                logger.error(
                    "No se pudo conectar a ningún endpoint de etcd. "
                    "Reintentando en %d s...",
                    backoff,
                )
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)
                continue

            # Conexión exitosa: resetear backoff
            backoff = 1

            try:
                # 1. Lectura inicial: obtener la IP actual del maestro
                self._initial_read(client)

                # 2. Watch reactivo: escuchar cambios en la clave
                self._watch_loop(client)

            except Exception as err:
                self.pool_mgr.invalidate_pool()
                logger.error(
                    "Error en la sesión de watch: %s. Reconectando...", err
                )
            finally:
                try:
                    client.close()
                except Exception:
                    pass

    def stop(self):
        """Solicita al hilo que termine."""
        self._stop_event.set()

    # ── Conexión con round-robin ────────────────────────────

    def _connect(self):
        """Intenta conectarse a cada endpoint en orden.

        Returns:
            etcd3.Etcd3Client si tiene éxito, None si todos fallan.
        """
        for host, port in self.endpoints:
            try:
                client = etcd3.client(host=host, port=port)
                # Verificar que la conexión funciona
                client.status()
                logger.info("Conectado a etcd en %s:%d", host, port)
                return client
            except Exception as err:
                logger.warning(
                    "No se pudo conectar a etcd %s:%d — %s",
                    host,
                    port,
                    err,
                )
        return None

    # ── Lectura inicial ─────────────────────────────────────

    def _initial_read(self, client):
        """Lee la clave del maestro y, si existe, inicializa el pool."""
        master_ip_bytes, metadata = client.get(self.key)
        # C-2 Fix: capture revision for gap-free watch
        self._last_revision = 0
        if metadata is not None:
            self._last_revision = metadata.mod_revision or 0
        if master_ip_bytes:
            master_ip = _decode_master_ip(master_ip_bytes)
            if self.pool_mgr.current_ip != master_ip:
                logger.info(
                    "Lectura inicial: maestro actual → %s", master_ip
                )
                self.pool_mgr.initialize_pool(master_ip)
        else:
            self.pool_mgr.invalidate_pool()
            logger.warning(
                "No hay clave de maestro activa en etcd (%s). "
                "Esperando a que un sidecar publique el líder...",
                self.key,
            )

    # ── Watch reactivo ──────────────────────────────────────

    def _watch_loop(self, client):
        """Mantiene un watch sobre la clave del maestro y reacciona
        a eventos PUT (nuevo maestro) y DELETE (lease expirado)."""
        logger.info("Registrando watcher en clave '%s'...", self.key)
        # C-2 Fix: start watching from the last known revision to avoid
        # missing events between the GET and the watch registration
        watch_kwargs = {}
        if self._last_revision > 0:
            watch_kwargs["start_revision"] = self._last_revision + 1
        events_iterator, cancel = client.watch(self.key, **watch_kwargs)

        try:
            for event in events_iterator:
                if self._stop_event.is_set():
                    break

                if isinstance(event, etcd3.events.PutEvent):
                    new_ip = _decode_master_ip(event.value)
                    logger.info(
                        "──── EVENTO PUT ──── Nuevo maestro detectado: %s",
                        new_ip,
                    )
                    if self.pool_mgr.current_ip != new_ip:
                        self.pool_mgr.initialize_pool(new_ip)
                    else:
                        logger.info(
                            "La IP no cambió (%s); "
                            "probablemente es una renovación de lease.",
                            new_ip,
                        )

                elif isinstance(event, etcd3.events.DeleteEvent):
                    logger.warning(
                        "──── EVENTO DELETE ──── "
                        "El maestro fue eliminado de etcd (lease expirado). "
                        "Esperando a que un sidecar elija nuevo líder..."
                    )
                    self.pool_mgr.invalidate_pool()

        finally:
            cancel()
            logger.info("Watcher cancelado.")
