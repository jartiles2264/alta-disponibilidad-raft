"""
Cliente etcd v3 sobre la API HTTP/JSON (gRPC-gateway).

¿Por que no se usa la libreria `etcd3` del enunciado?
Porque `python-etcd3` esta abandonada y es incompatible con protobuf >= 4
(en este equipo hay protobuf 6.x), asi que ni siquiera importa. En vez de
pelear con versiones, se habla directo con el gateway HTTP que etcd expone
en el mismo puerto de cliente (2379). Solo se necesita `requests`.

Endpoints usados:
    POST /v3/kv/range         leer
    POST /v3/kv/put           escribir
    POST /v3/kv/txn           transaccion atomica (eleccion de lider)
    POST /v3/lease/grant      crear lease con TTL
    POST /v3/lease/keepalive  renovar lease (latido)
    POST /v3/lease/revoke     soltar lease (renuncia voluntaria)
    POST /v3/watch            stream de cambios

Todas las claves y valores viajan en base64.
"""

import base64
import json
import time

import requests


def _b64(texto: str) -> str:
    return base64.b64encode(texto.encode("utf-8")).decode("ascii")


def _des64(dato: str) -> str:
    return base64.b64decode(dato).decode("utf-8")


class EtcdError(Exception):
    """No se pudo hablar con ningun endpoint de etcd."""


class EtcdClient:
    """
    Cliente contra un cluster etcd. Recibe varios endpoints y rota entre
    ellos: si el nodo local cae, las peticiones siguen saliendo por otro.
    """

    def __init__(self, endpoints, timeout=None):
        if isinstance(endpoints, str):
            endpoints = [e.strip() for e in endpoints.split(",") if e.strip()]
        self.endpoints = []
        for e in endpoints:
            e = e.strip().rstrip("/")
            if not e.startswith("http://") and not e.startswith("https://"):
                e = "http://" + e
            self.endpoints.append(e)
        # (conectar, leer): conectar rapido para descartar en seguida un
        # nodo caido y pasar al siguiente, pero dar margen de lectura
        # porque etcd hace fsync a disco y bajo carga tarda.
        self.timeout = timeout or (1.5, 5.0)
        self._actual = 0  # indice del endpoint que respondio la ultima vez

    # ------------------------------------------------------------------
    # Transporte
    # ------------------------------------------------------------------
    def _post(self, ruta, payload):
        """Intenta la peticion en cada endpoint hasta que uno responda."""
        ultimo_error = None
        total = len(self.endpoints)
        for salto in range(total):
            indice = (self._actual + salto) % total
            url = self.endpoints[indice] + ruta
            try:
                resp = requests.post(url, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                self._actual = indice  # recordar el que funciona
                return resp.json()
            except Exception as exc:  # noqa: BLE001
                ultimo_error = exc
        raise EtcdError(f"ningun endpoint de etcd respondio ({ultimo_error})")

    # ------------------------------------------------------------------
    # Claves
    # ------------------------------------------------------------------
    def get(self, clave):
        """Devuelve el valor como str, o None si la clave no existe."""
        resp = self._post("/v3/kv/range", {"key": _b64(clave)})
        kvs = resp.get("kvs")
        if not kvs:
            return None
        return _des64(kvs[0]["value"])

    def get_prefijo(self, prefijo):
        """
        Devuelve {clave: valor} de todo lo que empiece con `prefijo`.

        En etcd un rango por prefijo se pide con range_end = el prefijo con
        su ultimo byte incrementado en uno.
        """
        fin = prefijo[:-1] + chr(ord(prefijo[-1]) + 1)
        resp = self._post(
            "/v3/kv/range", {"key": _b64(prefijo), "range_end": _b64(fin)}
        )
        return {
            _des64(kv["key"]): _des64(kv["value"])
            for kv in resp.get("kvs", [])
            if kv.get("value")
        }

    def get_con_lease(self, clave):
        """Devuelve (valor, lease_id) o (None, 0). Sirve para saber si la
        clave del maestro sigue amarrada a un lease vivo."""
        resp = self._post("/v3/kv/range", {"key": _b64(clave)})
        kvs = resp.get("kvs")
        if not kvs:
            return None, 0
        return _des64(kvs[0]["value"]), int(kvs[0].get("lease", "0"))

    def put(self, clave, valor, lease=None):
        payload = {"key": _b64(clave), "value": _b64(valor)}
        if lease:
            payload["lease"] = str(lease)
        self._post("/v3/kv/put", payload)

    def delete(self, clave):
        self._post("/v3/kv/deleterange", {"key": _b64(clave)})

    # ------------------------------------------------------------------
    # Leases (arrendamientos con TTL)
    # ------------------------------------------------------------------
    def lease_grant(self, ttl_segundos):
        resp = self._post("/v3/lease/grant", {"TTL": str(int(ttl_segundos))})
        return int(resp["ID"])

    def lease_keepalive(self, lease_id):
        """Renueva el lease. Devuelve el TTL restante; 0 significa que el
        lease ya expiro o fue revocado (perdimos el liderazgo)."""
        resp = self._post("/v3/lease/keepalive", {"ID": str(lease_id)})
        resultado = resp.get("result", resp)
        return int(resultado.get("TTL", "0") or 0)

    def lease_revoke(self, lease_id):
        """Suelta el lease: borra de inmediato las claves asociadas.
        Se usa para renunciar limpio en vez de esperar a que expire."""
        try:
            self._post("/v3/lease/revoke", {"ID": str(lease_id)})
        except EtcdError:
            pass  # si etcd no responde, el lease expirara solo por TTL

    # ------------------------------------------------------------------
    # Eleccion de lider
    # ------------------------------------------------------------------
    def crear_si_no_existe(self, clave, valor, lease):
        """
        Transaccion atomica: escribe la clave SOLO si nadie la tiene.

        Compara create_revision == 0, que en etcd significa "esta clave no
        existe". Si dos nodos lo intentan a la vez, Raft serializa las dos
        transacciones y solo una ve la clave vacia: exactamente lo que hace
        falta para elegir maestro sin split-brain.

        Devuelve True si ganamos la eleccion.
        """
        payload = {
            "compare": [
                {
                    "result": "EQUAL",
                    "target": "CREATE",
                    "key": _b64(clave),
                    "createRevision": "0",
                }
            ],
            "success": [
                {
                    "requestPut": {
                        "key": _b64(clave),
                        "value": _b64(valor),
                        "lease": str(lease),
                    }
                }
            ],
            "failure": [{"requestRange": {"key": _b64(clave)}}],
        }
        resp = self._post("/v3/kv/txn", payload)
        return bool(resp.get("succeeded", False))

    def salud(self):
        """Lista de (endpoint, ok) para diagnostico."""
        estado = []
        for ep in self.endpoints:
            try:
                resp = requests.get(ep + "/health", timeout=self.timeout)
                estado.append((ep, resp.json().get("health") == "true"))
            except Exception:  # noqa: BLE001
                estado.append((ep, False))
        return estado

    # ------------------------------------------------------------------
    # Watcher
    # ------------------------------------------------------------------
    def watch(self, clave, al_cambiar, detener=None, intervalo_respaldo=0.5):
        """
        Vigila una clave y llama `al_cambiar(tipo, valor)` en cada evento,
        donde tipo es "PUT" or "DELETE" (valor es None en DELETE).

        Usa el stream real de /v3/watch. Si el stream se corta o el gateway
        no coopera, cae automaticamente a sondeo cada `intervalo_respaldo`
        segundos, de modo que el failover nunca depende de que el stream
        sobreviva justo en el momento de la caida.

        Bloquea: se ejecuta en un hilo aparte.
        """
        while detener is None or not detener.is_set():
            try:
                self._watch_stream(clave, al_cambiar, detener)
            except Exception as exc:  # noqa: BLE001
                print(f"[etcd] watch por stream fallo ({exc}); paso a sondeo")
                self._watch_sondeo(clave, al_cambiar, detener, intervalo_respaldo)

    def _watch_stream(self, clave, al_cambiar, detener):
        url = self.endpoints[self._actual] + "/v3/watch"
        payload = {"create_request": {"key": _b64(clave)}}
        # sin limite de lectura: el stream queda abierto a proposito hasta
        # que llegue un evento (self.timeout[0] es el limite de conexion)
        with requests.post(
            url, json=payload, stream=True, timeout=(self.timeout[0], None)
        ) as resp:
            resp.raise_for_status()
            for linea in resp.iter_lines():
                if detener is not None and detener.is_set():
                    return
                if not linea:
                    continue
                mensaje = json.loads(linea).get("result", {})
                for evento in mensaje.get("events", []):
                    tipo = evento.get("type", "PUT")  # PUT se omite en JSON
                    kv = evento.get("kv", {})
                    valor = _des64(kv["value"]) if kv.get("value") else None
                    al_cambiar(tipo, valor)

    def _watch_sondeo(self, clave, al_cambiar, detener, intervalo):
        """
        Respaldo: relee la clave y avisa cuando cambia.

        El centinela hace que la PRIMERA lectura siempre notifique. Es la
        diferencia entre recuperarse de un failover y quedarse colgado: si
        el stream se corta justo cuando cae el maestro, al entrar aqui la
        clave ya tiene la IP nueva, y si esa primera lectura se tomara como
        "estado inicial" en vez de como un cambio, nadie avisaria nunca y
        la aplicacion se quedaria esperando un evento que ya paso.
        """
        anterior = object()
        while detener is None or not detener.is_set():
            try:
                actual = self.get(clave)
                if actual != anterior:
                    anterior = actual
                    al_cambiar("DELETE" if actual is None else "PUT", actual)
                    if actual is not None:
                        return  # hay maestro: se reintenta el stream
            except EtcdError:
                pass
            time.sleep(intervalo)
