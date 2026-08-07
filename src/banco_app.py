"""
banco_app.py — Aplicación Bancaria Transaccional
=================================================

Programa principal del cliente bancario.  Realiza transferencias
ACID contra el clúster MySQL de alta disponibilidad, gestionando
automáticamente la reconexión tras un failover mediante el
watcher de etcd y el pool dinámico.

Características clave:
  - **Idempotencia**: cada transferencia lleva un UUID único
    (``idempotency_key``).  Si un reintento detecta que la clave
    ya existe en la tabla ``transacciones``, la operación no se
    ejecuta dos veces.
  - **Reintentos inteligentes**: espera al evento ``pool_ready``
    del watcher en lugar de un ``time.sleep()`` ciego.
  - **Separación de errores**: errores de negocio (saldo
    insuficiente) no generan reintentos; errores de
    infraestructura (conexión perdida) sí.
  - **Modo Prueba de Oro** (``--prueba-oro``): bucle automático
    de transferencias continuas para la evaluación.
  - **Menú interactivo**: consultar saldos, transferir, ver
    historial.
  - **Shutdown limpio**: maneja SIGINT/SIGTERM.

Autor: Persona 4
Proyecto: Examen Tercer Parcial — Sistemas Distribuidos
"""

import argparse
import logging
import math
import os
import signal
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation

import mysql.connector
from dotenv import load_dotenv

from db_pool import DynamicDatabasePool
from etcd_watcher import EtcdWatcherThread

# ── Cargar variables de entorno ──────────────────────────────
# Busca .env en el mismo directorio que este script.
load_dotenv(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
)

# ── Logging ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)-12s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("banco_app")

# ── Configuración ────────────────────────────────────────────

RETRY_MAX = int(os.getenv("RETRY_MAX", "15"))
FAILOVER_WAIT = int(os.getenv("FAILOVER_WAIT", "30"))
TRANSFER_INTERVAL = float(os.getenv("TRANSFER_INTERVAL", "1"))

# ── Señal de apagado limpio ──────────────────────────────────

_shutdown = False


def _signal_handler(signum, frame):
    global _shutdown
    logger.info("Señal de apagado recibida (%s). Cerrando...", signum)
    _shutdown = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ═════════════════════════════════════════════════════════════
# FUNCIONES DE NEGOCIO
# ═════════════════════════════════════════════════════════════


def transferir_dinero(cuenta_origen, cuenta_destino, monto):
    """Ejecuta una transferencia bancaria con reintentos idempotentes.

    Genera un ``idempotency_key`` (UUID) para esta operación.
    En cada reintento verifica si la transacción ya fue confirmada.

    Args:
        cuenta_origen: número de la cuenta que envía el dinero.
        cuenta_destino: número de la cuenta que recibe.
        monto: cantidad a transferir (Decimal o float).

    Returns:
        True si la transferencia fue exitosa, False si falló
        tras agotar todos los reintentos.
    """
    try:
        monto = Decimal(str(monto))
    except (TypeError, ValueError, InvalidOperation):
        logger.error("El monto debe ser numérico.")
        return False
    if not monto.is_finite() or monto <= 0:
        logger.error("El monto debe ser finito y mayor que cero.")
        return False
    if cuenta_origen == cuenta_destino:
        logger.error("Las cuentas de origen y destino deben ser distintas.")
        return False

    pool_mgr = DynamicDatabasePool.get_instance()
    idempotency_key = str(uuid.uuid4())

    logger.info(
        "Iniciando transferencia $%.2f  %s → %s  [uid=%s]",
        monto,
        cuenta_origen,
        cuenta_destino,
        idempotency_key[:8],
    )

    for intento in range(1, RETRY_MAX + 1):
        if _shutdown:
            logger.info("Apagado solicitado; abortando transferencia.")
            return False

        conn = None
        try:
            conn = pool_mgr.get_connection()
            cursor = conn.cursor()

            # ── Transacción ACID ─────────────────────────────
            conn.start_transaction()

            # ── Verificación de idempotencia ─────────────────
            cursor.execute(
                "SELECT id FROM transacciones "
                "WHERE uid = %s AND estado = 'COMPLETADO'",
                (idempotency_key,),
            )
            if cursor.fetchone() is not None:
                logger.info(
                    "Transferencia [uid=%s] ya fue confirmada previamente. "
                    "Omitiendo reintento.",
                    idempotency_key[:8],
                )
                return True

            # I-2 Fix: Lock both accounts in deterministic order to prevent deadlocks
            cuentas_ordenadas = sorted([cuenta_origen, cuenta_destino])
            saldos = {}
            for cuenta in cuentas_ordenadas:
                cursor.execute(
                    "SELECT saldo FROM cuentas "
                    "WHERE numero_cuenta = %s FOR UPDATE",
                    (cuenta,),
                )
                row = cursor.fetchone()
                if row is None:
                    conn.rollback()
                    logger.error("Cuenta '%s' no existe.", cuenta)
                    return False  # Error de negocio: no reintentar
                saldos[cuenta] = row[0]

            saldo_origen = saldos[cuenta_origen]
            if saldo_origen < monto:
                conn.rollback()
                logger.error(
                    "Saldo insuficiente en %s: $%.2f < $%.2f",
                    cuenta_origen, saldo_origen, monto,
                )
                return False

            # 2. Descontar del origen
            cursor.execute(
                "UPDATE cuentas SET saldo = saldo - %s "
                "WHERE numero_cuenta = %s",
                (monto, cuenta_origen),
            )

            # 3. Acreditar al destino
            cursor.execute(
                "UPDATE cuentas SET saldo = saldo + %s "
                "WHERE numero_cuenta = %s",
                (monto, cuenta_destino),
            )

            if cursor.rowcount != 1:
                conn.rollback()
                logger.error(
                    "Cuenta destino '%s' no existe. Transferencia revertida.",
                    cuenta_destino,
                )
                return False   # Error de negocio: no reintentar

            # 4. Registrar la transacción con su clave de idempotencia y el nodo maestro actual
            current_ip = pool_mgr.current_ip
            cursor.execute(
                "INSERT INTO transacciones "
                "(uid, cuenta_origen, cuenta_destino, "
                " monto, estado, nodo_maestro) "
                "VALUES (%s, %s, %s, %s, 'COMPLETADO', %s)",
                (idempotency_key, cuenta_origen, cuenta_destino, monto, current_ip),
            )

            conn.commit()
            logger.info(
                "✅ Transferencia exitosa: $%.2f  %s → %s  "
                "[uid=%s, intento=%d, nodo=%s]",
                monto,
                cuenta_origen,
                cuenta_destino,
                idempotency_key[:8],
                intento,
                current_ip
            )
            return True

        except mysql.connector.Error as err:
            # ── Error de infraestructura → reintentar ────────
            logger.warning(
                "⚠️  Intento %d/%d falló (infra): %s",
                intento,
                RETRY_MAX,
                err,
            )
            _safe_rollback(conn)
            _wait_for_pool(pool_mgr)

        except Exception as err:
            # ── Error inesperado ─────────────────────────────
            logger.error(
                "❌ Intento %d/%d falló (inesperado): %s",
                intento,
                RETRY_MAX,
                err,
            )
            _safe_rollback(conn)
            _wait_for_pool(pool_mgr)

        finally:
            _safe_close(conn)

    logger.error(
        "❌ Transferencia fallida tras %d intentos [uid=%s].",
        RETRY_MAX,
        idempotency_key[:8],
    )
    return False


def consultar_saldos():
    """Muestra los saldos de todas las cuentas."""
    pool_mgr = DynamicDatabasePool.get_instance()
    conn = None
    try:
        conn = pool_mgr.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT numero_cuenta, cliente, saldo FROM cuentas "
            "ORDER BY numero_cuenta"
        )
        rows = cursor.fetchall()
        conn.commit()
        print("\n" + "=" * 55)
        print(f"  {'Cuenta':<12} {'Cliente':<22} {'Saldo':>12}")
        print("-" * 55)
        for cuenta, cliente, saldo in rows:
            print(f"  {cuenta:<12} {cliente:<22} ${saldo:>10,.2f}")
        print("=" * 55 + "\n")
    except Exception as err:
        logger.error("Error al consultar saldos: %s", err)
    finally:
        _safe_close(conn)


def consultar_historial(limit=20):
    """Muestra las últimas transacciones registradas."""
    pool_mgr = DynamicDatabasePool.get_instance()
    conn = None
    try:
        conn = pool_mgr.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, cuenta_origen, cuenta_destino, monto, "
            "       fecha, estado "
            "FROM transacciones ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        rows = cursor.fetchall()
        conn.commit()
        print("\n" + "=" * 80)
        print(
            f"  {'ID':<6} {'Origen':<10} {'Destino':<10} "
            f"{'Monto':>10} {'Fecha':<20} {'Estado':<12}"
        )
        print("-" * 80)
        for tid, ori, dest, monto, fecha, estado in rows:
            print(
                f"  {tid:<6} {ori:<10} {dest:<10} "
                f"${monto:>9,.2f} {str(fecha):<20} {estado:<12}"
            )
        print("=" * 80 + "\n")
    except Exception as err:
        logger.error("Error al consultar historial: %s", err)
    finally:
        _safe_close(conn)


# ═════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════


def _safe_rollback(conn):
    """Intenta hacer rollback sin lanzar excepciones."""
    if conn:
        try:
            conn.rollback()
        except Exception:
            pass


def _safe_close(conn):
    """Intenta cerrar la conexión y liberar del tracking."""
    if conn:
        try:
            pool_mgr = DynamicDatabasePool.get_instance()
            pool_mgr.release_connection(conn)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _wait_for_pool(pool_mgr):
    """Espera a que el pool esté listo tras un failover.

    Usa el evento pool_ready del pool en lugar de un sleep fijo.
    Si el evento ya está señalizado (no hubo failover, solo un
    error transitorio), espera un delay configurable.
    """
    if pool_mgr.pool_ready.is_set():
        # No hay reconstruccion en curso: fallo transitorio o el
        # watcher aun no se entera. Espera fija para no quemar los
        # reintentos.
        time.sleep(2)  # RETRY_DELAY hardcoded o mantener la const original
        return
    logger.info(
        "Failover en curso. Esperando nuevo maestro (max %d s)...",
        FAILOVER_WAIT,
    )
    if pool_mgr.pool_ready.wait(timeout=FAILOVER_WAIT):
        logger.info("Pool reconstruido hacia %s. Reanudando.",
                    pool_mgr.current_ip)
    else:
        logger.warning("El pool no se reconstruyo en %d s.",
                       FAILOVER_WAIT)


# ═════════════════════════════════════════════════════════════
# MODOS DE EJECUCIÓN
# ═════════════════════════════════════════════════════════════


def modo_prueba_oro():
    """Bucle de transferencias continuas para la evaluación.

    Envía $1.00 de la cuenta 1111 a la 2222 cada
    TRANSFER_INTERVAL segundos, imprimiendo logs claros para
    captura de evidencias.
    """
    logger.info("=" * 60)
    logger.info("   MODO PRUEBA DE ORO — Transferencias Continuas")
    logger.info("   Intervalo: %.1f s | Ctrl+C para detener", TRANSFER_INTERVAL)
    logger.info("=" * 60)

    contador = 0
    exitosas = 0
    fallidas = 0

    while not _shutdown:
        contador += 1
        logger.info("─── Transferencia #%d ───", contador)
        resultado = transferir_dinero("1111", "2222", 1.00)
        if resultado:
            exitosas += 1
        else:
            fallidas += 1

        if contador % 10 == 0:
            logger.info(
                "📊 Resumen parcial: %d exitosas, %d fallidas de %d total",
                exitosas,
                fallidas,
                contador,
            )

        if not _shutdown:
            time.sleep(TRANSFER_INTERVAL)

    logger.info("=" * 60)
    logger.info(
        "📊 RESUMEN FINAL: %d exitosas, %d fallidas de %d total",
        exitosas,
        fallidas,
        contador,
    )
    logger.info("=" * 60)


def modo_interactivo():
    """Menú interactivo para operaciones manuales."""
    while not _shutdown:
        print("\n┌─────────────────────────────────────┐")
        print("│   SISTEMA BANCARIO — MENÚ PRINCIPAL │")
        print("├─────────────────────────────────────┤")
        print("│  1. Consultar saldos                │")
        print("│  2. Realizar transferencia           │")
        print("│  3. Ver historial de transacciones   │")
        print("│  4. Iniciar prueba de oro (continuo) │")
        print("│  5. Salir                            │")
        print("└─────────────────────────────────────┘")

        try:
            opcion = input("  Seleccione una opción: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if opcion == "1":
            consultar_saldos()

        elif opcion == "2":
            try:
                origen = input("  Cuenta origen: ").strip()
                destino = input("  Cuenta destino: ").strip()
                monto = float(input("  Monto: $").strip())
                transferir_dinero(origen, destino, monto)
            except (ValueError, EOFError, KeyboardInterrupt):
                print("  Entrada inválida.")

        elif opcion == "3":
            consultar_historial()

        elif opcion == "4":
            modo_prueba_oro()

        elif opcion == "5":
            break

        else:
            print("  Opción no válida.")

    logger.info("Saliendo del sistema bancario...")


# ═════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ═════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description="Aplicación Bancaria — Alta Disponibilidad con Raft"
    )
    parser.add_argument(
        "--prueba-oro",
        action="store_true",
        help="Ejecutar en modo de prueba continua (Prueba de Oro)",
    )
    args = parser.parse_args()

    pool_mgr = DynamicDatabasePool.get_instance()

    # 1. Iniciar el hilo watcher de etcd
    logger.info("Iniciando watcher de etcd...")
    watcher = EtcdWatcherThread()
    watcher.start()

    # 2. Esperar a que el pool esté listo (máx 15 s)
    logger.info(
        "Esperando conexión inicial al maestro MySQL vía etcd..."
    )
    if not pool_mgr.pool_ready.wait(timeout=15):
        logger.error(
            "No se pudo obtener la IP del maestro desde etcd en 15 s. "
            "Verifique que el clúster etcd esté activo y que un "
            "sidecar haya publicado la clave del maestro."
        )
        sys.exit(1)

    logger.info(
        "Conectado al maestro MySQL en %s.", pool_mgr.current_ip
    )

    # 3. Ejecutar el modo seleccionado
    if args.prueba_oro:
        modo_prueba_oro()
    else:
        modo_interactivo()

    # 4. Apagado limpio
    watcher.stop()
    logger.info("Aplicación bancaria finalizada.")


if __name__ == "__main__":
    main()
