"""
Pool de conexiones dinamico: se reconstruye en caliente cuando cambia
la IP del maestro.
"""

import threading
import time

import mysql.connector.pooling


class DynamicDatabasePool:
    _instancia = None
    _lock_clase = threading.Lock()

    def __init__(self):
        self._lock = threading.RLock()
        self._pool = None
        self._generacion = 0
        self.ip_actual = None
        self.puerto = 3306
        self.usuario = "admin"
        self.password = "password"
        self.base = "banco_db"
        self.tamano = 8

    @classmethod
    def get_instance(cls):
        with cls._lock_clase:
            if cls._instancia is None:
                cls._instancia = cls()
            return cls._instancia

    def configurar(self, usuario, password, base="banco_db",
                   puerto=3306, tamano=8):
        self.usuario = usuario
        self.password = password
        self.base = base
        self.puerto = int(puerto)
        self.tamano = int(tamano)

    # ------------------------------------------------------------------
    def reconstruir(self, ip):
        """Cierra el pool actual y crea uno nuevo apuntando a `ip`."""
        with self._lock:
            if ip == self.ip_actual and self._pool is not None:
                return
            self._cerrar_pool()
            self._generacion += 1
            nombre = f"bancopool_{self._generacion}"
            print(f"[POOL] Construyendo pool '{nombre}' hacia {ip}:{self.puerto}", flush=True)
            self._pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name=nombre,
                pool_size=self.tamano,
                pool_reset_session=True,
                host=ip,
                port=self.puerto,
                user=self.usuario,
                password=self.password,
                database=self.base,
                connection_timeout=3,
                autocommit=False,
            )
            self.ip_actual = ip
            print(f"[POOL] Listo. Maestro activo: {ip}", flush=True)

    def invalidar(self):
        """El maestro desaparecio de etcd: no hay a donde escribir todavia."""
        with self._lock:
            if self._pool is not None:
                print("[POOL] Sin maestro; el pool queda invalidado.", flush=True)
            self._cerrar_pool()
            self.ip_actual = None

    def _cerrar_pool(self):
        if self._pool is None:
            return
        try:
            # vacia las conexiones que aun apuntan al servidor anterior
            while True:
                conexion = self._pool.get_connection()
                conexion.close()
        except Exception:  # noqa: BLE001
            pass
        self._pool = None

    # ------------------------------------------------------------------
    def get_connection(self, espera_max=30):
        """
        Entrega una conexion del pool vigente. Si estamos en pleno failover,
        espera (hasta `espera_max`) a que el watcher instale el nuevo pool.
        """
        limite = time.time() + espera_max
        while time.time() < limite:
            with self._lock:
                if self._pool is not None:
                    return self._pool.get_connection()
            time.sleep(0.2)
        raise RuntimeError(
            f"No hubo un maestro disponible en {espera_max}s"
        )

    @property
    def hay_maestro(self):
        with self._lock:
            return self._pool is not None
