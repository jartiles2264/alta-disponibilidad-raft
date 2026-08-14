-- Esquema del banco.
--
-- Se ejecuta UNA sola vez contra el maestro: como el cluster replica por
-- GTID, la creacion de la base y los datos de prueba viajan solos a los
-- otros cuatro nodos. No hace falta mysqldump ni copiar nada a mano.

CREATE DATABASE IF NOT EXISTS banco_db;
USE banco_db;

CREATE TABLE IF NOT EXISTS cuentas (
    numero_cuenta VARCHAR(20) PRIMARY KEY,
    cliente       VARCHAR(100)   NOT NULL,
    saldo         DECIMAL(12, 2) NOT NULL CHECK (saldo >= 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS transacciones (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    cuenta_origen  VARCHAR(20),
    cuenta_destino VARCHAR(20),
    monto          DECIMAL(12, 2) NOT NULL,
    fecha          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estado         VARCHAR(20)    NOT NULL,
    nodo_maestro   VARCHAR(45),
    INDEX idx_fecha (fecha)
) ENGINE=InnoDB;

-- Datos de prueba: la suma de los dos saldos debe seguir siendo 1500.00
-- despues del failover. Ese es el invariante que se verifica al final.
INSERT INTO cuentas (numero_cuenta, cliente, saldo) VALUES ('1111', 'Juan Perez', 1000.00)
    ON DUPLICATE KEY UPDATE saldo = 1000.00;
INSERT INTO cuentas (numero_cuenta, cliente, saldo) VALUES ('2222', 'Maria Gomez', 500.00)
    ON DUPLICATE KEY UPDATE saldo = 500.00;
