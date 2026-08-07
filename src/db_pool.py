"""
db_pool.py — Administrador del Pool de Conexiones Dinámico
==========================================================

Gestiona un pool de conexiones MySQL que puede ser destruido y
reconstruido en caliente cuando el nodo maestro cambia.

Patrón Singleton thread-safe: una sola instancia compartida entre
el hilo principal (banco_app) y el hilo observador (etcd_watcher).

Mejoras sobre el código guía del examen:
  - Destrucción limpia del pool anterior al reconstruir
  - threading.Event para sincronización (evita sleep arbitrarios)
  - Credenciales desde variables de entorno (.env)
  - Logging estructurado con timestamps para evidencias
  - Manejo de excepciones tipado

Autor: Persona 4
Proyecto: Examen Tercer Parcial — Sistemas Distribuidos
"""

import os
import logging
import threading

import mysql.connector
import mysql.connector.pooling

# ── Logging ──────────────────────────────────────────────────

logger = logging.getLogger("db_pool")


class DynamicDatabasePool:
    """Pool de conexiones MySQL reconstruible en caliente.

    Atributos de clase:
        _instance: referencia Singleton.
        _lock: candado para operaciones sobre el pool.

    Atributos de instancia:
        pool: el objeto MySQLConnectionPool activo (o None).
        current_ip: IP del maestro MySQL al que apunta el pool.
        pool_ready: evento que se señaliza cuando el pool está
                    listo para entregar conexiones.  El hilo
                    principal puede llamar pool_ready.wait() en
                    lugar de time.sleep() para esperar tras un
                    failover.
    """

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.pool = None
        self.current_ip = None
        self.pool_ready = threading.Event()
        self._active_connections = set()  # C-3 Fix: track in-flight connections
        # Leer credenciales del entorno una sola vez
        self._user = os.getenv("MYSQL_USER", "banco_app")
        self._password = os.getenv("MYSQL_PASSWORD", "")
        self._database = os.getenv("MYSQL_DATABASE", "banco_db")
        self._port = int(os.getenv("MYSQL_PORT", "3306"))
        self._pool_size = int(os.getenv("POOL_SIZE", "10"))

    # ── Singleton ────────────────────────────────────────────

    @classmethod
    def get_instance(cls):
        """Devuelve la instancia única del pool (thread-safe)."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    # ── Gestión del pool ─────────────────────────────────────

    def initialize_pool(self, ip):
        """Destruye el pool anterior (si existe) y crea uno nuevo
        apuntando a la IP indicada.

        Args:
            ip: dirección IP del nodo MySQL maestro.
        """
        if not self._password:
            raise RuntimeError("MYSQL_PASSWORD es obligatorio.")
        with self._lock:
            # Señalar que el pool NO está listo mientras se reconstruye
            self.pool_ready.clear()

            # 1. Destruir el pool anterior de forma limpia
            if self.pool is not None:
                logger.warning(
                    "Destruyendo pool anterior (apuntaba a %s)...",
                    self.current_ip,
                )
                self._drain_pool()

            # 2. Crear el pool nuevo sin publicar un estado parcial
            logger.info(
                "Inicializando pool de conexiones → %s:%d ...",
                ip,
                self._port,
            )
            try:
                new_pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name="bancopool",
                    pool_size=self._pool_size,
                    pool_reset_session=True,
                    host=ip,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    database=self._database,
                    # Timeouts cortos para detectar caídas rápido
                    connection_timeout=5,
                    autocommit=False,  # I-1 Fix: explicit transaction control
                )
                logger.info("Pool de conexiones creado con éxito.")
            except mysql.connector.Error as err:
                logger.error(
                    "No se pudo crear el pool hacia %s: %s", ip, err
                )
                self.pool = None
                self.current_ip = None
                raise

            # 3. Publicar la IP solo cuando el pool existe
            self.pool = new_pool
            self.current_ip = ip
            self.pool_ready.set()

    def get_connection(self):
        """Obtiene una conexión del pool.

        Returns:
            mysql.connector.connection.MySQLConnection

        Raises:
            RuntimeError: si el pool no está inicializado.
            mysql.connector.Error: si no hay conexiones disponibles.
        """
        if not self._password:
            raise RuntimeError("MYSQL_PASSWORD es obligatorio.")
        with self._lock:
            if not self.pool_ready.is_set() or self.pool is None:
                raise RuntimeError(
                    "El pool no está disponible durante el failover."
                )
            conn = self.pool.get_connection()
            self._active_connections.add(id(conn))
            return conn

    def release_connection(self, conn):
        """Remove a connection from active tracking."""
        with self._lock:
            self._active_connections.discard(id(conn))

    def invalidate_pool(self):
        """Bloquea operaciones y descarta conexiones al maestro anterior."""
        with self._lock:
            self.pool_ready.clear()
            if self.pool is not None:
                logger.warning(
                    "Invalidando pool hacia %s por cambio de maestro.",
                    self.current_ip,
                )
                self._drain_pool()
            self.current_ip = None

    # ── Helpers internos ─────────────────────────────────────

    def _drain_pool(self):
        """Intenta cerrar las conexiones del pool anterior.

        mysql.connector.pooling no expone un método close() del
        pool directamente, así que vaciamos las conexiones de la
        cola interna y las cerramos una por una.
        """
        try:
            # Acceder a la cola interna del pool
            # (implementación de CPython de mysql-connector-python)
            while not self.pool._cnx_queue.empty():
                cnx = self.pool._cnx_queue.get_nowait()
                try:
                    cnx.close()
                except Exception:
                    pass
            logger.info("Pool anterior drenado correctamente.")
        except Exception as err:
            logger.warning(
                "No se pudo drenar el pool anterior completamente: %s",
                err,
            )
        finally:
            self._active_connections.clear()  # C-3 Fix: clear tracking
            self.pool = None
