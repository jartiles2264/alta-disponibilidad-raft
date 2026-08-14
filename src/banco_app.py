"""
Aplicacion bancaria transaccional (la "Prueba de Oro").

Ejecuta un bucle continuo de transferencias contra el maestro que indique
etcd. Cuando el maestro cae, el watcher reconstruye el pool con la IP del
nuevo maestro y la aplicacion reintenta la transaccion interrumpida sin
que nadie toque nada.
"""

import argparse
import os
import signal
import sys
import time
from pathlib import Path

import mysql.connector

# Importaciones locales
from db_pool import DynamicDatabasePool
from etcd_watcher import EtcdWatcherThread

# Errores que significan "este servidor ya no sirve, busca al nuevo maestro"
ERRORES_DE_FAILOVER = {
    2003,  # no se puede conectar
    2006,  # el servidor se fue
    2013,  # conexion perdida durante la consulta
    1290,  # servidor en modo --super-read-only (nos degradaron)
    1836,  # servidor en modo --read-only
    1047,  # servidor no disponible
}


class Estadisticas:
    def __init__(self):
        self.exitosas = 0
        self.fallidas = 0
        self.inicio = time.time()
        self.primer_fallo = None
        self.interrupciones = []

    def registrar_exito(self):
        self.exitosas += 1
        if self.primer_fallo is not None:
            duracion = time.time() - self.primer_fallo
            self.interrupciones.append(duracion)
            print("\n" + "=" * 62)
            print(f"  SERVICIO RESTABLECIDO - interrupcion de {duracion:.1f}s")
            print("=" * 62 + "\n", flush=True)
            self.primer_fallo = None

    def registrar_fallo(self):
        self.fallidas += 1
        if self.primer_fallo is None:
            self.primer_fallo = time.time()

    def resumen(self):
        minutos = (time.time() - self.inicio) / 60
        print("\n" + "=" * 62)
        print("  RESUMEN DE LA PRUEBA")
        print("=" * 62)
        print(f"  Duracion            : {minutos:.1f} min")
        print(f"  Transferencias OK   : {self.exitosas}")
        print(f"  Intentos fallidos   : {self.fallidas}")
        if self.interrupciones:
            print(f"  Failovers superados : {len(self.interrupciones)}")
            for i, dur in enumerate(self.interrupciones, 1):
                print(f"    #{i}: {dur:.1f}s sin servicio")
            print(f"  Peor interrupcion   : {max(self.interrupciones):.1f}s")
        else:
            print("  Failovers superados : ninguno (no hubo caidas)")
        print("=" * 62, flush=True)


def transferir(pool, watcher, origen, destino, monto, stats, intentos=8):
    """
    Transferencia ACID con reintento transparente ante failover.

    El bloque SQL es el clasico: bloquear la cuenta origen, descontar,
    abonar y registrar. Lo que agrega valor aqui es el manejo del error:
    si el fallo es de infraestructura se refresca la IP del maestro y se
    reintenta; si es de negocio (saldo insuficiente) se aborta de una vez.
    """
    for intento in range(1, intentos + 1):
        conexion = None
        try:
            conexion = pool.get_connection()
            cursor = conexion.cursor()
            conexion.start_transaction()

            cursor.execute(
                "SELECT saldo FROM cuentas WHERE numero_cuenta = %s FOR UPDATE",
                (origen,),
            )
            fila = cursor.fetchone()
            if fila is None:
                raise ValueError(f"La cuenta {origen} no existe")
            if fila[0] < monto:
                conexion.rollback()
                return "SIN_SALDO"

            cursor.execute(
                "UPDATE cuentas SET saldo = saldo - %s WHERE numero_cuenta = %s",
                (monto, origen),
            )
            cursor.execute(
                "UPDATE cuentas SET saldo = saldo + %s WHERE numero_cuenta = %s",
                (monto, destino),
            )
            cursor.execute(
                "INSERT INTO transacciones "
                "(cuenta_origen, cuenta_destino, monto, estado, nodo_maestro) "
                "VALUES (%s, %s, %s, 'COMPLETADO', %s)",
                (origen, destino, monto, pool.ip_actual),
            )

            conexion.commit()
            cursor.close()
            stats.registrar_exito()
            return "OK"

        except mysql.connector.Error as err:
            stats.registrar_fallo()
            codigo = getattr(err, "errno", None)
            print(f"[INTENTO {intento}] Error {codigo}: {str(err)[:90]}", flush=True)

            if conexion is not None:
                try:
                    conexion.rollback()
                except Exception:  # noqa: BLE001
                    pass  # la conexion ya estaba muerta

            if codigo in ERRORES_DE_FAILOVER or codigo is None:
                # El maestro cayo o nos degradaron: preguntar a etcd quien
                # manda ahora en vez de esperar pasivamente.
                watcher.refrescar_ahora()
                time.sleep(0.5)
            else:
                return "ERROR"

        except RuntimeError as err:
            # No hay pool porque el failover sigue en curso. En vez de
            # abandonar la transferencia, se vuelve a preguntar a etcd
            # quien manda y se reintenta.
            stats.registrar_fallo()
            print(f"[INTENTO {intento}] {err}", flush=True)
            watcher.refrescar_ahora()
            time.sleep(0.5)

        except Exception as err:  # noqa: BLE001
            stats.registrar_fallo()
            print(f"[INTENTO {intento}] {type(err).__name__}: {err}", flush=True)
            if conexion is not None:
                try:
                    conexion.rollback()
                except Exception:  # noqa: BLE001
                    pass
            return "ERROR"

        finally:
            if conexion is not None:
                try:
                    conexion.close()
                except Exception:  # noqa: BLE001
                    pass

    return "AGOTADO"


def mostrar_saldos(pool, titulo="SALDOS ACTUALES"):
    """Lee y pinta el estado de las cuentas desde el maestro vigente."""
    conexion = pool.get_connection()
    cursor = conexion.cursor()
    cursor.execute(
        "SELECT numero_cuenta, cliente, saldo FROM cuentas ORDER BY numero_cuenta"
    )
    filas = cursor.fetchall()
    cursor.execute("SELECT COUNT(*) FROM transacciones")
    total = cursor.fetchone()[0]
    cursor.close()
    conexion.close()

    print(f"\n  {titulo}   (maestro: {pool.ip_actual})")
    print("  " + "-" * 52)
    suma = 0
    for numero, cliente, saldo in filas:
        print(f"  {numero:<10} {cliente:<24} {saldo:>12,.2f}")
        suma += saldo
    print("  " + "-" * 52)
    print(f"  {'TOTAL':<35} {suma:>12,.2f}")
    print(f"  Transacciones registradas: {total}\n", flush=True)
    return suma


def modo_manual(pool, watcher):
    """
    Transferencias dictadas por teclado.
    """
    mostrar_saldos(pool)
    print("  Escribe la transferencia como:  ORIGEN DESTINO MONTO")
    print("  Ejemplo:  1111 2222 50")
    print("  Comandos: 'saldos' para releer, 'salir' para terminar\n", flush=True)

    while True:
        try:
            entrada = input("  banco> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not entrada:
            continue
        if entrada.lower() in ("salir", "exit", "quit"):
            return
        if entrada.lower() in ("saldos", "s"):
            mostrar_saldos(pool)
            continue

        partes = entrada.split()
        if len(partes) != 3:
            print("  Formato: ORIGEN DESTINO MONTO   (ej. 1111 2222 50)", flush=True)
            continue

        origen, destino, texto_monto = partes
        try:
            monto = float(texto_monto)
        except ValueError:
            print(f"  '{texto_monto}' no es un monto valido.", flush=True)
            continue

        stats = Estadisticas()
        resultado = transferir(pool, watcher, origen, destino, monto, stats)
        if resultado == "OK":
            print(f"  OK: ${monto:,.2f} de {origen} a {destino}", flush=True)
            mostrar_saldos(pool, "SALDOS DESPUES DE LA TRANSFERENCIA")
        elif resultado == "SIN_SALDO":
            print(f"  RECHAZADA: la cuenta {origen} no tiene saldo suficiente.", flush=True)
        else:
            print("  La transferencia no pudo completarse.", flush=True)


def crear_esquema(pool):
    ruta = Path(__file__).parent / "schema.sql"
    sin_comentarios = "\n".join(
        linea for linea in ruta.read_text(encoding="utf-8").splitlines()
        if not linea.strip().startswith("--")
    )
    sentencias = [s.strip() for s in sin_comentarios.split(";") if s.strip()]
    conexion = pool.get_connection()
    cursor = conexion.cursor()
    for sentencia in sentencias:
        cursor.execute(sentencia)
    conexion.commit()
    cursor.close()
    conexion.close()
    print("[INIT] Esquema creado en el maestro. Se replicara a los 4 nodos.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Cliente bancario HA")
    parser.add_argument("--init", action="store_true",
                        help="crear el esquema y salir")
    parser.add_argument("--saldos", action="store_true",
                        help="mostrar los saldos y salir")
    parser.add_argument("--reset", action="store_true",
                        help="devolver los saldos a 1000 y 500")
    parser.add_argument("--manual", action="store_true",
                        help="transferencias dictadas por teclado")
    parser.add_argument("--transferir", nargs=3,
                        metavar=("ORIGEN", "DESTINO", "MONTO"),
                        help="hacer una sola transferencia y salir")
    parser.add_argument("--tps", type=float, default=2.0,
                        help="transferencias por segundo (por defecto 2)")
    parser.add_argument("--monto", type=float, default=1.00)
    args = parser.parse_args()

    endpoints = os.environ.get("ETCD_ENDPOINTS")
    if not endpoints:
        sys.exit("Falta ETCD_ENDPOINTS (ej. http://172.20.10.3:2379,...)")

    pool = DynamicDatabasePool.get_instance()
    
    usuario = os.environ.get("APP_USER") or os.environ.get("MYSQL_USER", "banco_app")
    password = os.environ.get("APP_PASSWORD") or os.environ.get("MYSQL_PASSWORD", "bancopass123")
    base = os.environ.get("MYSQL_DATABASE", "banco_db") if not args.init else "mysql"
    puerto = int(os.environ.get("MYSQL_PORT", "3306"))
    tamano = int(os.environ.get("POOL_SIZE", "8"))
    
    pool.configurar(
        usuario=usuario,
        password=password,
        base=base,
        puerto=puerto,
        tamano=tamano,
    )

    watcher = EtcdWatcherThread(endpoints, pool)
    watcher.start()

    print("Esperando a que etcd indique quien es el maestro...", flush=True)
    espera = time.time() + 60
    while not pool.hay_maestro and time.time() < espera:
        time.sleep(0.3)
    if not pool.hay_maestro:
        sys.exit("Ningun nodo se declaro maestro en 60s. Revisa los sidecars.")

    if args.init:
        crear_esquema(pool)
        return

    if args.saldos:
        mostrar_saldos(pool)
        return

    if args.reset:
        conexion = pool.get_connection()
        cursor = conexion.cursor()
        cursor.execute(
            "UPDATE cuentas SET saldo = 1000.00 WHERE numero_cuenta = '1111'"
        )
        cursor.execute(
            "UPDATE cuentas SET saldo = 500.00 WHERE numero_cuenta = '2222'"
        )
        conexion.commit()
        cursor.close()
        conexion.close()
        print("[RESET] Saldos devueltos a los valores iniciales.", flush=True)
        mostrar_saldos(pool)
        return

    if args.transferir:
        origen, destino, texto_monto = args.transferir
        stats = Estadisticas()
        mostrar_saldos(pool, "SALDOS ANTES")
        resultado = transferir(pool, watcher, origen, destino,
                               float(texto_monto), stats)
        if resultado == "OK":
            print(f"  OK: ${float(texto_monto):,.2f} de {origen} a {destino}", flush=True)
            mostrar_saldos(pool, "SALDOS DESPUES")
        elif resultado == "SIN_SALDO":
            print(f"  RECHAZADA: la cuenta {origen} no tiene saldo suficiente.", flush=True)
        else:
            print("  La transferencia no pudo completarse.", flush=True)
        return

    if args.manual:
        modo_manual(pool, watcher)
        return

    stats = Estadisticas()
    signal.signal(signal.SIGINT, lambda *_: (stats.resumen(), sys.exit(0)))

    origen, destino = "1111", "2222"
    intervalo = 1.0 / args.tps
    print(f"\n--- Transferencias continuas: {args.tps}/s "
          f"de ${args.monto:.2f} ---")
    print("--- Ctrl+C para detener y ver el resumen ---\n", flush=True)

    while True:
        resultado = transferir(
            pool, watcher, origen, destino, args.monto, stats
        )
        if resultado == "OK":
            print(f"[OK #{stats.exitosas}] ${args.monto:.2f}  "
                  f"{origen} -> {destino}   (maestro {pool.ip_actual})", flush=True)
        elif resultado == "SIN_SALDO":
            origen, destino = destino, origen
            print(f"[INFO] Sentido invertido: ahora {origen} -> {destino}", flush=True)
        time.sleep(intervalo)


if __name__ == "__main__":
    main()
