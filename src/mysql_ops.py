"""
Operaciones sobre el MySQL local que necesita el agente sidecar:
salud, promocion a maestro, degradacion a replica y reconfiguracion
de la replicacion GTID en caliente.

Sintaxis de MySQL 8.4: REPLICA / SOURCE (los viejos SLAVE / MASTER
fueron eliminados en esta version, no solo desaconsejados).
"""

import re

import pymysql


class MySQLOps:
    def __init__(self, host, puerto, usuario, password,
                 repl_usuario, repl_password, app_usuario, app_password):
        self.host = host
        self.puerto = int(puerto)
        self.usuario = usuario
        self.password = password
        self.repl_usuario = repl_usuario
        self.repl_password = repl_password
        self.app_usuario = app_usuario
        self.app_password = app_password

    # ------------------------------------------------------------------
    # Conexion
    # ------------------------------------------------------------------
    def _conectar(self, timeout=3):
        return pymysql.connect(
            host=self.host,
            port=self.puerto,
            user=self.usuario,
            password=self.password,
            connect_timeout=timeout,
            read_timeout=timeout * 2,
            write_timeout=timeout * 2,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _ejecutar(self, sentencias):
        """Ejecuta una lista de sentencias en una sola conexion."""
        with self._conectar() as conn, conn.cursor() as cur:
            for sql in sentencias:
                cur.execute(sql)

    def _consultar_uno(self, sql):
        with self._conectar() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()

    # ------------------------------------------------------------------
    # Estado
    # ------------------------------------------------------------------
    def esta_vivo(self):
        try:
            self._consultar_uno("SELECT 1 AS ok")
            return True
        except Exception:  # noqa: BLE001
            return False

    def es_escribible(self):
        """True si este nodo acepta escrituras (o sea, actua de maestro)."""
        fila = self._consultar_uno(
            "SELECT @@GLOBAL.read_only AS ro, @@GLOBAL.super_read_only AS sro"
        )
        return int(fila["ro"]) == 0 and int(fila["sro"]) == 0

    def gtid_ejecutado(self):
        fila = self._consultar_uno("SELECT @@GLOBAL.gtid_executed AS g")
        return (fila["g"] or "").replace("\n", "")

    def transacciones_aplicadas(self):
        """
        Cuenta cuantas transacciones lleva aplicadas este nodo sumando los
        rangos de gtid_executed ("uuid:1-540,otro-uuid:1-3" -> 543).

        Se usa para decidir quien esta mas al dia: el nodo con mas
        transacciones aplicadas es el mejor candidato a maestro, porque
        promover a uno atrasado significa perder transacciones.
        """
        total = 0
        for tramo in re.findall(r":(\d+)(?:-(\d+))?", self.gtid_ejecutado()):
            inicio = int(tramo[0])
            fin = int(tramo[1]) if tramo[1] else inicio
            total += fin - inicio + 1
        return total

    def estado_replica(self):
        """SHOW REPLICA STATUS como dict, o None si no esta replicando."""
        try:
            return self._consultar_uno("SHOW REPLICA STATUS")
        except Exception:  # noqa: BLE001
            return None

    def fuente_actual(self):
        # En MySQL 8.4 es Source_Host
        estado = self.estado_replica()
        if not estado:
            return None
        # En algunas versiones de mysql, SHOW SLAVE STATUS aun devuelve Master_Host,
        # pero en 8.4 es Source_Host. Soportamos ambos para mayor compatibilidad.
        return estado.get("Source_Host") or estado.get("Master_Host")

    def replicacion_rota(self):
        """
        Devuelve el mensaje de error si la replicacion esta caida por un
        problema de datos (no por red). El caso tipico tras un failover es
        el error 1236: el viejo maestro tiene transacciones que el nuevo
        nunca recibio, asi que sus historiales GTID divergieron.
        """
        estado = self.estado_replica()
        if not estado:
            return None
        # Soportamos tanto nomenclatura replica (MySQL 8.4) como slave (MySQL anterior)
        replica_sql_running = estado.get("Replica_SQL_Running") or estado.get("Slave_SQL_Running")
        replica_io_running = estado.get("Replica_IO_Running") or estado.get("Slave_IO_Running")
        
        if replica_sql_running == "Yes" and replica_io_running == "Yes":
            return None
            
        for campo in ("Last_IO_Error", "Last_SQL_Error", "Last_IO_Errno", "Last_SQL_Errno"):
            mensaje = estado.get(campo)
            if mensaje:
                return str(mensaje)
        return None

    def relay_log_pendiente(self):
        """
        Transacciones que ya se recibieron del maestro pero aun no se
        aplican localmente. Antes de promover hay que dejar esto en cero
        para no perderlas.
        """
        estado = self.estado_replica()
        if not estado:
            return ""
        # Soportamos tanto Retrieved_Gtid_Set como Retrieved_Gtid_Set de versiones anteriores
        recibido = (estado.get("Retrieved_Gtid_Set") or "").replace("\n", "")
        if not recibido:
            return ""
        fila = self._consultar_uno(
            f"SELECT GTID_SUBTRACT('{recibido}', @@GLOBAL.gtid_executed) AS falta"
        )
        return (fila["falta"] or "").replace("\n", "")

    # ------------------------------------------------------------------
    # Cambios de rol
    # ------------------------------------------------------------------
    def promover_a_maestro(self):
        """
        Convierte este nodo en maestro de escritura.

        RESET REPLICA ALL borra la configuracion de replicacion: sin eso el
        nodo intentaria reconectarse al maestro muerto en cuanto vuelva.
        El orden de los read_only importa: super_read_only debe apagarse
        antes que read_only.
        """
        # Intentamos REPLICA y si falla caemos a SLAVE (para compatibilidad de versiones)
        try:
            self._ejecutar([
                "STOP REPLICA",
                "RESET REPLICA ALL",
                "SET GLOBAL super_read_only = OFF",
                "SET GLOBAL read_only = OFF",
            ])
        except pymysql.MySQLError:
            self._ejecutar([
                "STOP SLAVE",
                "RESET SLAVE ALL",
                "SET GLOBAL super_read_only = OFF",
                "SET GLOBAL read_only = OFF",
            ])

    def degradar_a_replica(self, ip_maestro, puerto_maestro=3306):
        """
        Pone el nodo en solo lectura y lo engancha al maestro indicado.

        GET_SOURCE_PUBLIC_KEY=1 es obligatorio: MySQL 8.4 usa
        caching_sha2_password y sin esta opcion la replica no puede
        autenticarse sobre una conexion sin TLS (falla con "Authentication
        plugin caching_sha2_password reported error").
        """
        try:
            # Sintaxis MySQL 8.4
            self._ejecutar([
                "SET GLOBAL read_only = ON",
                "SET GLOBAL super_read_only = ON",
                "STOP REPLICA",
                f"""CHANGE REPLICATION SOURCE TO
                        SOURCE_HOST='{ip_maestro}',
                        SOURCE_PORT={int(puerto_maestro)},
                        SOURCE_USER='{self.repl_usuario}',
                        SOURCE_PASSWORD='{self.repl_password}',
                        SOURCE_AUTO_POSITION=1,
                        SOURCE_CONNECT_RETRY=1,
                        SOURCE_RETRY_COUNT=0,
                        GET_SOURCE_PUBLIC_KEY=1""",
                "START REPLICA",
            ])
        except pymysql.MySQLError:
            # Sintaxis MySQL 8.0 y anteriores
            self._ejecutar([
                "SET GLOBAL read_only = ON",
                "SET GLOBAL super_read_only = ON",
                "STOP SLAVE",
                f"""CHANGE MASTER TO
                        MASTER_HOST='{ip_maestro}',
                        MASTER_PORT={int(puerto_maestro)},
                        MASTER_USER='{self.repl_usuario}',
                        MASTER_PASSWORD='{self.repl_password}',
                        MASTER_AUTO_POSITION=1,
                        MASTER_CONNECT_RETRY=1,
                        MASTER_RETRY_COUNT=0,
                        GET_MASTER_PUBLIC_KEY=1""",
                "START SLAVE",
            ])

    def solo_lectura(self):
        self._ejecutar([
            "SET GLOBAL read_only = ON",
            "SET GLOBAL super_read_only = ON",
        ])

    def resync_desde_cero(self, ip_maestro, puerto_maestro=3306):
        """
        Ultimo recurso cuando el historial GTID divergio (error 1236).

        Ocurre cuando el viejo maestro alcanzo a confirmar transacciones que
        nunca llegaron a replicarse antes de caer: su gtid_executed contiene
        cosas que el nuevo maestro no tiene, y la replicacion se niega a
        arrancar. Se descarta el historial local y se vuelve a enganchar.

        Ojo: esto descarta esas transacciones huerfanas. Es lo correcto en
        un failover, porque el nuevo maestro ya es la fuente de verdad y la
        aplicacion cliente reintenta lo que no recibio confirmacion.
        """
        try:
            self._ejecutar([
                "STOP REPLICA",
                "RESET REPLICA ALL",
                "SET GLOBAL read_only = ON",
                "SET GLOBAL super_read_only = ON",
                "RESET BINARY LOGS AND GTID",
            ])
        except pymysql.MySQLError:
            self._ejecutar([
                "STOP SLAVE",
                "RESET SLAVE ALL",
                "SET GLOBAL read_only = ON",
                "SET GLOBAL super_read_only = ON",
                "RESET BINARY LOGS AND GTID",
            ])
        self.degradar_a_replica(ip_maestro, puerto_maestro)

    # ------------------------------------------------------------------
    # Usuarios del cluster
    # ------------------------------------------------------------------
    def asegurar_usuarios(self):
        """
        Crea el usuario de replicacion y el de la aplicacion.

        Lo ejecuta solo quien es maestro; como son sentencias con GTID, se
        replican solas a los demas nodos. Asi el Paso 1 del enunciado (crear
        replica_user a mano en cada PC) queda automatizado.
        """
        self._ejecutar([
            f"CREATE USER IF NOT EXISTS '{self.repl_usuario}'@'%' "
            f"IDENTIFIED BY '{self.repl_password}'",
            f"GRANT REPLICATION SLAVE ON *.* TO '{self.repl_usuario}'@'%'",
            f"CREATE USER IF NOT EXISTS '{self.app_usuario}'@'%' "
            f"IDENTIFIED BY '{self.app_password}'",
            f"GRANT ALL PRIVILEGES ON *.* TO '{self.app_usuario}'@'%' "
            f"WITH GRANT OPTION",
            "FLUSH PRIVILEGES",
        ])
