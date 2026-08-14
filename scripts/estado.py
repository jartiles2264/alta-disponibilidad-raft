"""
Panel de estado del cluster: una sola pantalla con todo lo que hay que
mirar durante la demostracion.

    python scripts/estado.py            # una foto
    python scripts/estado.py --seguir   # se refresca cada 2 segundos
"""

import argparse
import base64
import os
import sys
from pathlib import Path

import requests

try:
    import pymysql
except ImportError:
    sys.exit("Falta pymysql:  pip install pymysql")

RAIZ = Path(__file__).resolve().parent.parent
CLAVE_MAESTRO = os.environ.get("ETCD_KEY", "/banco/mysql/primary")

VERDE, ROJO, AMARILLO, GRIS, FIN = (
    "\033[92m", "\033[91m", "\033[93m", "\033[90m", "\033[0m"
)


def leer_config():
    cfg = {}
    # Intentamos leer de .env o de cluster.env
    env_path = RAIZ / ".env"
    if not env_path.exists():
        env_path = RAIZ / "cluster.env"
    
    if not env_path.exists():
        sys.exit(f"No se encontro el archivo .env ni cluster.env en {RAIZ}")
        
    for linea in env_path.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        cfg[clave.strip()] = valor.strip()
    return cfg


def maestro_segun_etcd(ips):
    """Pregunta a cualquier nodo de etcd que responda quien es el maestro."""
    payload = {"key": base64.b64encode(CLAVE_MAESTRO.encode()).decode()}
    for ip in ips:
        try:
            resp = requests.post(
                f"http://{ip}:2379/v3/kv/range", json=payload, timeout=2
            )
            kvs = resp.json().get("kvs")
            if not kvs:
                return None, ip
            return base64.b64decode(kvs[0]["value"]).decode(), ip
        except Exception:  # noqa: BLE001
            continue
    return None, None


def etcd_vivo(ip):
    try:
        resp = requests.get(f"http://{ip}:2379/health", timeout=2)
        return resp.json().get("health") == "true"
    except Exception:  # noqa: BLE001
        return False


def inspeccionar_mysql(ip, password):
    """Devuelve el estado del MySQL de un nodo, o None si no responde."""
    import socket
    hosts_a_probar = [ip]
    mis_ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            mis_ips.add(info[4][0])
    except Exception:
        pass

    if ip in mis_ips or ip == "localhost":
        if "127.0.0.1" not in hosts_a_probar:
            hosts_a_probar.append("127.0.0.1")

    conn = None
    for host in hosts_a_probar:
        try:
            conn = pymysql.connect(
                host=host, port=3306, user="root", password=password,
                connect_timeout=1, read_timeout=2,
                cursorclass=pymysql.cursors.DictCursor,
            )
            break
        except Exception:
            continue

    if conn is None:
        return None

    datos = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.read_only ro, @@GLOBAL.super_read_only sro")
            fila = cur.fetchone()
            datos["escribible"] = int(fila["ro"]) == 0 and int(fila["sro"]) == 0

            cur.execute("SELECT @@GLOBAL.gtid_executed g")
            gtid = (cur.fetchone()["g"] or "").replace("\n", "")
            total = 0
            import re
            for ini, fin in re.findall(r":(\d+)(?:-(\d+))?", gtid):
                total += (int(fin) if fin else int(ini)) - int(ini) + 1
            datos["transacciones"] = total

            cur.execute("SHOW REPLICA STATUS")
            rep = cur.fetchone()
            if rep:
                datos["fuente"] = rep.get("Source_Host")
                datos["io"] = rep.get("Replica_IO_Running")
                datos["sql"] = rep.get("Replica_SQL_Running")
                datos["retraso"] = rep.get("Seconds_Behind_Source")
                datos["error"] = (rep.get("Last_IO_Error")
                                  or rep.get("Last_SQL_Error") or "")
            else:
                datos["fuente"] = None

            try:
                cur.execute(
                    "SELECT SUM(saldo) t, COUNT(*) n FROM banco_db.cuentas"
                )
                fila = cur.fetchone()
                datos["suma_saldos"] = fila["t"]
                cur.execute("SELECT COUNT(*) n FROM banco_db.transacciones")
                datos["n_transacciones"] = cur.fetchone()["n"]
                # saldo de cada cuenta por separado, para poder comparar
                # cuenta por cuenta entre nodos y no solo el total
                cur.execute(
                    "SELECT numero_cuenta, saldo FROM banco_db.cuentas "
                    "ORDER BY numero_cuenta"
                )
                datos["cuentas"] = {
                    f["numero_cuenta"]: f["saldo"] for f in cur.fetchall()
                }
            except Exception:  # noqa: BLE001
                datos["suma_saldos"] = None
                datos["n_transacciones"] = None
                datos["cuentas"] = {}
    finally:
        conn.close()
    return datos


def pintar_cuentas(por_nodo):
    """
    Tabla de saldo por cuenta y por nodo.
    """
    if len(por_nodo) < 2:
        return

    cuentas = sorted({c for datos in por_nodo.values() for c in datos})
    if not cuentas:
        return

    print()
    print("  SALDO DE CADA CUENTA EN CADA NODO")
    print("  " + "-" * 68)
    encabezado = "  {:<10}".format("Cuenta")
    for numero in sorted(por_nodo):
        encabezado += f"{'nodo ' + str(numero):>12}"
    print(encabezado)

    for cuenta in cuentas:
        fila = f"  {cuenta:<10}"
        valores = []
        for numero in sorted(por_nodo):
            valor = por_nodo[numero].get(cuenta)
            valores.append(valor)
            fila += f"{valor if valor is not None else '-':>12}"
        distintos = {str(v) for v in valores if v is not None}
        marca = f"  {VERDE}=={FIN}" if len(distintos) == 1 else f"  {ROJO}!={FIN}"
        print(fila + marca)

    print("  " + "-" * 68)
    todas_iguales = all(
        len({str(por_nodo[n].get(c)) for n in por_nodo
             if por_nodo[n].get(c) is not None}) == 1
        for c in cuentas
    )
    if todas_iguales:
        print(f"  {VERDE}IDENTICAS{FIN}  Cada cuenta vale exactamente lo mismo "
              f"en todos los nodos")
    else:
        print(f"  {ROJO}DESCUADRE{FIN}  Hay cuentas con distinto saldo "
              f"segun el nodo")


def pintar(cfg, detalle=False):
    ips = [cfg[f"LAN_IP_{n}"] for n in range(1, 6) if cfg.get(f"LAN_IP_{n}")]
    # Fallback to NODE_IP if LAN_IP is not defined
    if not ips:
        ips = [cfg[f"NODE{n}_IP"] for n in range(1, 6) if cfg.get(f"NODE{n}_IP")]
    
    password = cfg.get("MYSQL_ROOT_PASSWORD", "rootpassword")

    maestro, consultado = maestro_segun_etcd(ips)
    print("=" * 92)
    if maestro:
        print(f"  MAESTRO SEGUN etcd: {VERDE}{maestro}{FIN}"
              f"   {GRIS}(respondio {consultado}){FIN}")
    elif consultado:
        print(f"  {AMARILLO}SIN MAESTRO: la clave esta libre, "
              f"hay una eleccion en curso{FIN}")
    else:
        print(f"  {ROJO}etcd no responde en ningun nodo{FIN}")
    print("=" * 92)
    print(f"  {'Nodo':<5} {'IP':<16} {'etcd':<7} {'MySQL':<8} {'Rol':<10} "
          f"{'Replica de':<16} {'Trans.':<8} {'Saldos':<10}")
    print("  " + "-" * 88)

    sumas, conteos, cuentas_por_nodo = {}, {}, {}
    for numero, ip in enumerate(ips, 1):
        salud_etcd = f"{VERDE}ok{FIN}" if etcd_vivo(ip) else f"{ROJO}--{FIN}"
        estado = inspeccionar_mysql(ip, password)

        if estado is None:
            print(f"  {numero:<5} {ip:<16} {salud_etcd:<16} "
                  f"{ROJO}caido{FIN}")
            continue

        if estado["escribible"]:
            rol = f"{VERDE}MAESTRO{FIN}" if ip == maestro else f"{ROJO}ESCRIBIBLE!{FIN}"
        else:
            rol = f"{GRIS}replica{FIN}"

        fuente = estado.get("fuente") or "-"
        if fuente != "-":
            corriendo = estado.get("io") == "Yes" and estado.get("sql") == "Yes"
            fuente = (f"{VERDE}{fuente}{FIN}" if corriendo
                      else f"{ROJO}{fuente} (rota){FIN}")

        suma = estado.get("suma_saldos")
        if suma is not None:
            sumas[numero] = suma
            conteos[numero] = estado.get("n_transacciones")
            cuentas_por_nodo[numero] = estado.get("cuentas", {})
        texto_suma = f"{suma}" if suma is not None else "-"

        print(f"  {numero:<5} {ip:<16} {salud_etcd:<16} {VERDE}ok{FIN}      "
              f"{rol:<19} {fuente:<25} "
              f"{estado['transacciones']:<8} {texto_suma:<10}")

        error = estado.get("error")
        if error:
            print(f"        {ROJO}> {error[:80]}{FIN}")

    print("  " + "-" * 88)

    if len(sumas) >= 2:
        distintos = set(str(v) for v in sumas.values())
        if len(distintos) == 1:
            print(f"  {VERDE}CONSISTENTE{FIN}  Todos los nodos con datos "
                  f"suman {list(distintos)[0]}")
        else:
            print(f"  {AMARILLO}DIVERGENCIA de saldos entre nodos:{FIN}")
            for numero, valor in sumas.items():
                print(f"      nodo {numero}: {valor} "
                      f"({conteos.get(numero)} transacciones)")
            print(f"  {GRIS}  Si acaba de haber un failover, es replicacion "
                  f"en curso: vuelve a mirar en unos segundos.{FIN}")

    if detalle:
        pintar_cuentas(cuentas_por_nodo)
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seguir", action="store_true",
                        help="refrescar cada 2 segundos")
    parser.add_argument("--cuentas", action="store_true",
                        help="mostrar el saldo de cada cuenta en cada nodo")
    args = parser.parse_args()

    os.system("")  # habilita los colores ANSI en la consola de Windows
    cfg = leer_config()

    if not args.seguir:
        pintar(cfg, args.cuentas)
        return

    import time
    try:
        while True:
            os.system("cls" if os.name == "nt" else "clear")
            pintar(cfg, args.cuentas)
            time.sleep(2)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
