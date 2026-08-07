"""MySQL high-availability sidecar agent."""

import ipaddress
import logging
import os
import signal
import time
import socket

import etcd3
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [sidecar:%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sidecar")

def validate_node_address(address):
    """Validate that address is a valid IP or resolvable hostname."""
    address = address.strip()
    if not address:
        raise ValueError("Address cannot be empty")
    try:
        ipaddress.ip_address(address)
        return  # Valid IP
    except ValueError:
        pass
    # Try as hostname
    try:
        socket.getaddrinfo(address, None)
    except socket.gaierror:
        raise ValueError(f"Cannot resolve hostname: {address!r}")

def _load_cluster_conf(path):
    """Read cluster-nodes.conf, la fuente unica de verdad de las IPs.

    Ver README-CONFIGURACION.md. Los valores de .env tienen prioridad;
    esto solo aporta los valores por defecto, para que no haya ninguna
    direccion escrita a mano en este archivo.
    """
    conf = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                conf[key.strip()] = value.strip()
    except OSError:
        logger.warning(
            "No se pudo leer %s; se usaran solo los valores de .env", path
        )
    return conf


CLUSTER_CONF = _load_cluster_conf(
    os.path.join(BASE_DIR, "cluster-nodes.conf")
)
_CONF_IPS = [
    CLUSTER_CONF[f"NODE{index}_IP"]
    for index in range(1, 6)
    if CLUSTER_CONF.get(f"NODE{index}_IP")
]
_CONF_CLIENT_PORT = CLUSTER_CONF.get("ETCD_CLIENT_PORT", "2379")

DEFAULT_NODES = ",".join(_CONF_IPS)
DEFAULT_ETCD_ENDPOINTS = ",".join(
    f"{ip}:{_CONF_CLIENT_PORT}" for ip in _CONF_IPS
)
DEFAULT_PRIMARY_IP = CLUSTER_CONF.get(
    f"NODE{CLUSTER_CONF.get('INITIAL_PRIMARY_NODE', '1')}_IP", ""
)

NODE_IP = os.getenv("NODE_IP", "").strip()
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1").strip()
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
SIDECAR_MYSQL_USER = os.getenv("SIDECAR_MYSQL_USER", "sidecar").strip()
SIDECAR_MYSQL_PASSWORD = os.getenv("SIDECAR_MYSQL_PASSWORD", "")
MYSQL_REPL_USER = os.getenv("MYSQL_REPL_USER", "replica_user").strip()
MYSQL_REPL_PASSWORD = os.getenv("MYSQL_REPL_PASSWORD", "")
MYSQL_NODES = [
    item.strip()
    for item in os.getenv("MYSQL_NODES", DEFAULT_NODES).split(",")
    if item.strip()
]
ETCD_ENDPOINTS_RAW = os.getenv("ETCD_ENDPOINTS", DEFAULT_ETCD_ENDPOINTS)
ETCD_KEY = os.getenv(
    "ETCD_KEY", os.getenv("PRIMARY_KEY", "/banco/mysql/primary")
).strip()
LEASE_TTL = int(os.getenv("LEASE_TTL", "5"))
CHECK_INTERVAL = float(os.getenv("CHECK_INTERVAL", "2"))
INITIAL_PRIMARY_IP = os.getenv(
    "INITIAL_PRIMARY_IP", DEFAULT_PRIMARY_IP
).strip()
# Ciclos que este nodo cede ante uno mas fresco antes de competir igual.
# Evita que el cluster se quede sin maestro si el sidecar del nodo mas
# actualizado no esta corriendo.
MAX_DEFERRALS = int(os.getenv("MAX_DEFERRALS", "5"))

current_role = "UNKNOWN"
current_primary_ip = None
_endpoint_cursor = 0
_deferrals = 0


def parse_endpoints(raw):
    """Parse a comma-separated host:port list."""
    endpoints = []
    for value in raw.split(","):
        value = value.strip()
        if not value:
            continue
        host, separator, port_text = value.rpartition(":")
        if not separator:
            host, port_text = value, "2379"
        if not host:
            raise ValueError(f"Invalid etcd endpoint: {value!r}")
        port = int(port_text)
        if not 1 <= port <= 65535:
            raise ValueError(f"Invalid etcd port: {port}")
        endpoints.append((host, port))
    if not endpoints:
        raise ValueError("ETCD_ENDPOINTS is empty")
    return endpoints


ETCD_ENDPOINTS = parse_endpoints(ETCD_ENDPOINTS_RAW)


def validate_config():
    """Fail fast when election configuration is unsafe."""
    if not NODE_IP:
        raise ValueError("NODE_IP is required")
    if not MYSQL_NODES:
        raise ValueError(
            "MYSQL_NODES esta vacio: define MYSQL_NODES en .env o "
            "asegurate de que cluster-nodes.conf exista en la raiz"
        )
    if not INITIAL_PRIMARY_IP:
        raise ValueError(
            "INITIAL_PRIMARY_IP esta vacio: define INITIAL_PRIMARY_IP en "
            ".env o INITIAL_PRIMARY_NODE en cluster-nodes.conf"
        )
    if MAX_DEFERRALS < 1:
        raise ValueError("MAX_DEFERRALS must be at least 1")
    validate_node_address(NODE_IP)
    validate_node_address(INITIAL_PRIMARY_IP)
    for node_ip in MYSQL_NODES:
        validate_node_address(node_ip)
    if NODE_IP not in MYSQL_NODES:
        raise ValueError("NODE_IP must be present in MYSQL_NODES")
    if LEASE_TTL < 3:
        raise ValueError("LEASE_TTL must be at least 3 seconds")
    if CHECK_INTERVAL <= 0 or CHECK_INTERVAL >= LEASE_TTL:
        raise ValueError("CHECK_INTERVAL must be between 0 and LEASE_TTL")
    if not SIDECAR_MYSQL_PASSWORD:
        raise ValueError("SIDECAR_MYSQL_PASSWORD is required")
    if not MYSQL_REPL_PASSWORD:
        raise ValueError("MYSQL_REPL_PASSWORD is required")


def get_db_connection(host=None):
    """Open an administrative connection to a MySQL node."""
    return pymysql.connect(
        host=host or MYSQL_HOST,
        port=MYSQL_PORT,
        user=SIDECAR_MYSQL_USER,
        password=SIDECAR_MYSQL_PASSWORD,
        autocommit=True,
        connect_timeout=2,
        read_timeout=3,
        write_timeout=3,
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_mysql_state(host=None):
    """Read health, read-only flags, GTID and replica status."""
    connection = get_db_connection(host)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT @@GLOBAL.server_id AS server_id, "
                "@@GLOBAL.read_only AS read_only, "
                "@@GLOBAL.super_read_only AS super_read_only, "
                "@@GLOBAL.gtid_executed AS gtid_executed"
            )
            state = cursor.fetchone()
            cursor.execute("SHOW REPLICA STATUS")
            state["replica"] = cursor.fetchone()
            return state
    finally:
        connection.close()


def is_healthy_candidate(state):
    """Allow a writable primary or a replica with a healthy SQL thread."""
    if not state["read_only"] and not state["super_read_only"]:
        return True
    replica = state.get("replica")
    return bool(
        replica
        and replica.get("Replica_SQL_Running") == "Yes"
        and int(replica.get("Last_SQL_Errno") or 0) == 0
    )


def _gtid_subset(connection, left, right):
    with connection.cursor() as cursor:
        cursor.execute("SELECT GTID_SUBSET(%s, %s) AS is_subset", (left, right))
        return bool(cursor.fetchone()["is_subset"])


def choose_freshest_candidate(bootstrap=False):
    """Choose a healthy node whose GTID set is not behind another node."""
    states = {}
    for node_ip in MYSQL_NODES:
        host = MYSQL_HOST if node_ip == NODE_IP else node_ip
        try:
            state = get_mysql_state(host)
            initial_local = (
                bootstrap
                and node_ip == NODE_IP
                and node_ip == INITIAL_PRIMARY_IP
            )
            recovering_local = (
                node_ip == NODE_IP
                and current_primary_ip == NODE_IP
                and not state.get("replica")
            )
            if (
                is_healthy_candidate(state)
                or initial_local
                or recovering_local
            ):
                states[node_ip] = state
        except Exception as error:
            logger.warning("MySQL node %s is not eligible: %s", node_ip, error)

    local_state = states.get(NODE_IP)
    if local_state is None:
        logger.warning("Local MySQL is not a healthy candidate")
        return None

    global _deferrals

    comparison = get_db_connection()
    try:
        local_gtid = local_state["gtid_executed"]
        fresher_nodes = []
        for node_ip, state in states.items():
            if node_ip == NODE_IP:
                continue
            remote_gtid = state["gtid_executed"]
            local_subset = _gtid_subset(comparison, local_gtid, remote_gtid)
            remote_subset = _gtid_subset(comparison, remote_gtid, local_gtid)
            if not local_subset and not remote_subset:
                # Historias GTID incompatibles: promover cualquiera de los
                # dos perderia transacciones confirmadas. Esto NUNCA se
                # salta, por muchos ciclos que pasen.
                logger.error("Historial GTID divergente frente a %s", node_ip)
                _deferrals = 0
                return None
            if local_subset and not remote_subset:
                fresher_nodes.append(node_ip)

        if fresher_nodes:
            # Solo se comprueba que el MySQL del otro nodo responda, no que
            # su sidecar este vivo. Si el nodo mas fresco tiene el proceso
            # caido, todos le ceden el paso y el cluster se queda sin
            # maestro indefinidamente. Tras varios ciclos cediendo sin que
            # aparezca la clave, se permite competir al siguiente.
            _deferrals += 1
            if _deferrals <= MAX_DEFERRALS:
                logger.warning(
                    "GTID local por detras de %s; cediendo (%d/%d)",
                    ", ".join(fresher_nodes),
                    _deferrals,
                    MAX_DEFERRALS,
                )
                return None
            logger.error(
                "Tras %d ciclos nadie reclamo el liderazgo. Se asume que el "
                "sidecar de %s no esta activo y se compite igualmente. "
                "ATENCION: puede perderse la ultima replicacion de ese nodo.",
                _deferrals,
                ", ".join(fresher_nodes),
            )

        # Los nodos igual de frescos compiten a la vez; etcd resuelve el
        # empate de forma atomica.
        _deferrals = 0
        return NODE_IP
    finally:
        comparison.close()


def fence_local_mysql(reason):
    """Force read-only mode whenever this node has no valid lease."""
    global current_role
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SET GLOBAL super_read_only = ON")
            cursor.execute("SET GLOBAL read_only = ON")
        if current_role != "FENCED":
            logger.warning("MySQL fenced in read-only mode: %s", reason)
        current_role = "FENCED"
    except Exception as error:
        logger.error("Could not fence MySQL (%s): %s", reason, error)
    finally:
        if connection is not None:
            connection.close()


def promote_to_primary():
    """Promote only after winning the atomic etcd transaction."""
    global current_role, current_primary_ip
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute("STOP REPLICA")
            except pymysql.MySQLError:
                pass
            cursor.execute("SET GLOBAL super_read_only = OFF")
            cursor.execute("SET GLOBAL read_only = OFF")
        current_role = "PRIMARY"
        current_primary_ip = NODE_IP
        logger.info("PROMOTION complete: %s is primary", NODE_IP)
    finally:
        connection.close()


def demote_to_replica(primary_ip):
    """Fence this node and point its replication at the published primary."""
    global current_role, current_primary_ip
    validate_node_address(primary_ip)
    if primary_ip == NODE_IP:
        return
    if current_role == "REPLICA" and current_primary_ip == primary_ip:
        return

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET GLOBAL super_read_only = ON")
            cursor.execute("SET GLOBAL read_only = ON")
            try:
                cursor.execute("STOP REPLICA")
            except pymysql.MySQLError:
                pass
            cursor.execute(
                "CHANGE REPLICATION SOURCE TO "
                "SOURCE_HOST=%s, SOURCE_PORT=%s, SOURCE_USER=%s, "
                "SOURCE_PASSWORD=%s, SOURCE_AUTO_POSITION=1",
                (
                    primary_ip,
                    MYSQL_PORT,
                    MYSQL_REPL_USER,
                    MYSQL_REPL_PASSWORD,
                ),
            )
            cursor.execute("START REPLICA")
        current_role = "REPLICA"
        current_primary_ip = primary_ip
        logger.info("REPLICA configured: %s follows %s", NODE_IP, primary_ip)
    except Exception:
        current_role = "FENCED"
        raise
    finally:
        connection.close()


def connect_etcd():
    """Try every etcd endpoint, rotating the first endpoint."""
    global _endpoint_cursor
    total = len(ETCD_ENDPOINTS)
    for offset in range(total):
        index = (_endpoint_cursor + offset) % total
        host, port = ETCD_ENDPOINTS[index]
        client = None
        try:
            client = etcd3.client(host=host, port=port, timeout=3)
            client.status()
            _endpoint_cursor = (index + 1) % total
            logger.info("Connected to etcd %s:%d", host, port)
            return client
        except Exception as error:
            logger.warning("etcd %s:%d unavailable: %s", host, port, error)
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
    return None


def hold_leadership(client, lease):
    """Keep the lease alive and fence MySQL on any coordination failure."""
    try:
        lease.refresh()  # C-1 Fix: extend lease window before slow promote
        promote_to_primary()
        while True:
            # C-1 Fix: refresh and verify BEFORE sleeping
            lease.refresh()
            value, _ = client.get(ETCD_KEY)
            if not value or value.decode("utf-8").strip() != NODE_IP:
                raise RuntimeError("leadership key changed")
            state = get_mysql_state()
            if state["read_only"] or state["super_read_only"]:
                logger.warning(
                    "El primario volvio a solo lectura (probable reinicio "
                    "de MySQL); reabriendo escrituras"
                )
                promote_to_primary()
            time.sleep(min(CHECK_INTERVAL, LEASE_TTL / 3))
    except Exception as error:
        fence_local_mysql(f"lease or quorum lost: {error}")
    finally:
        try:
            lease.revoke()
        except Exception:
            pass


def try_election(client, observed_primary):
    """Atomically claim the key when the local node is eligible."""
    local_state = get_mysql_state()
    bootstrap = not observed_primary
    if bootstrap:
        initial_allowed = (
            NODE_IP == INITIAL_PRIMARY_IP
            or (
                not local_state["read_only"]
                and not local_state["super_read_only"]
            )
        )
        if not initial_allowed:
            logger.info("Waiting for initial primary %s", INITIAL_PRIMARY_IP)
            return

    chosen_ip = choose_freshest_candidate(bootstrap=bootstrap)
    if chosen_ip != NODE_IP:
        logger.info("Preferred health/GTID candidate: %s", chosen_ip)
        return

    lease = client.lease(LEASE_TTL)
    success, _ = client.transaction(
        compare=[client.transactions.version(ETCD_KEY) == 0],
        success=[client.transactions.put(ETCD_KEY, NODE_IP, lease=lease)],
        failure=[],
    )
    if success:
        logger.info("Election won with lease TTL=%ds", LEASE_TTL)
        hold_leadership(client, lease)
    else:
        try:
            lease.revoke()
        except Exception:
            pass


def _handle_shutdown(signum, _frame):
    logger.warning("Signal %s received; fencing before shutdown", signum)
    raise KeyboardInterrupt


def main():
    """Run the sidecar coordination loop."""
    global current_primary_ip, current_role
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    validate_config()
    logger.info(
        "Sidecar started | node=%s | etcd=%s",
        NODE_IP,
        ",".join(f"{host}:{port}" for host, port in ETCD_ENDPOINTS),
    )
    observed_primary = False

    try:
        while True:
            client = connect_etcd()
            if client is None:
                fence_local_mysql("no etcd quorum access")
                time.sleep(CHECK_INTERVAL)
                continue

            try:
                value, _ = client.get(ETCD_KEY)
                if value:
                    primary_ip = value.decode("utf-8").strip()
                    validate_node_address(primary_ip)
                    observed_primary = True
                    if primary_ip == NODE_IP:
                        # The key still has a valid lease. A restarted
                        # process observes it until it expires or changes.
                        current_primary_ip = NODE_IP
                        current_role = "PRIMARY_OBSERVED"
                    else:
                        demote_to_replica(primary_ip)
                else:
                    fence_local_mysql("no primary key with a valid lease")
                    try_election(client, observed_primary)
            except Exception as error:
                fence_local_mysql(f"coordination error: {error}")
                logger.error("Coordination cycle failed: %s", error)
            finally:
                try:
                    client.close()
                except Exception:
                    pass

            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        fence_local_mysql("sidecar stopped")


if __name__ == "__main__":
    main()
