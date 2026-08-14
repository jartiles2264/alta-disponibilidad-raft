"""
Hilo observador de etcd.

Vigila /banco/mysql/primary y ordena reconstruir el pool de conexiones en
cuanto la IP del maestro cambia. Es lo que permite que la aplicacion
sobreviva al failover sin reiniciarse.
"""

import os
import sys
import threading
import time

# Importaciones locales
from etcd_client import EtcdClient, EtcdError

CLAVE_MAESTRO = os.environ.get("ETCD_KEY", "/banco/mysql/primary")


class EtcdWatcherThread(threading.Thread):
    def __init__(self, endpoints, pool):
        super().__init__(daemon=True)
        self.etcd = EtcdClient(endpoints)
        self.pool = pool
        self.detener = threading.Event()
        self.cambios = 0
        self.momento_caida = None  # cuando se quedo sin maestro

    def run(self):
        print("[WATCHER] Observando", CLAVE_MAESTRO, flush=True)
        self._sincronizar_inicial()
        while not self.detener.is_set():
            try:
                self.etcd.watch(CLAVE_MAESTRO, self._al_cambiar, self.detener)
            except Exception as exc:  # noqa: BLE001
                print(f"[WATCHER] Reintentando tras error: {exc}", flush=True)
                time.sleep(1)

    def _sincronizar_inicial(self):
        """Lee la IP del maestro al arrancar, antes de la primera transaccion."""
        while not self.detener.is_set():
            try:
                ip = self.etcd.get(CLAVE_MAESTRO)
                if ip:
                    self.pool.reconstruir(ip)
                    return
                print("[WATCHER] Todavia no hay maestro elegido; esperando...", flush=True)
            except EtcdError as exc:
                print(f"[WATCHER] etcd no responde ({exc}); reintento...", flush=True)
            time.sleep(1)

    def _al_cambiar(self, tipo, valor):
        if tipo == "DELETE" or valor is None:
            print("[WATCHER] El maestro desaparecio de etcd. Failover en curso...", flush=True)
            self.momento_caida = time.time()
            self.pool.invalidar()
            return

        if valor == self.pool.ip_actual:
            return

        self.cambios += 1
        if self.momento_caida:
            tardanza = time.time() - self.momento_caida
            print(f"[WATCHER] Nuevo maestro: {valor} "
                  f"(etcd tardo {tardanza:.1f}s en reelegir)", flush=True)
            self.momento_caida = None
        else:
            print(f"[WATCHER] Nuevo maestro: {valor}", flush=True)
        self.pool.reconstruir(valor)

    def refrescar_ahora(self):
        """
        Relectura inmediata bajo demanda.

        La aplicacion la invoca cuando una transaccion falla: puede que el
        maestro haya cambiado justo entre dos eventos del stream, y asi no
        se pierde un segundo esperando al watcher.
        """
        try:
            ip = self.etcd.get(CLAVE_MAESTRO)
        except EtcdError:
            return None
        if ip and ip != self.pool.ip_actual:
            self._al_cambiar("PUT", ip)
        elif not ip:
            self.pool.invalidar()
        return ip
