SET sql_log_bin = 0;
USE banco_db;

CREATE TABLE IF NOT EXISTS cuentas (
    numero_cuenta VARCHAR(20) PRIMARY KEY,
    cliente VARCHAR(100) NOT NULL,
    saldo DECIMAL(12, 2) NOT NULL CHECK (saldo >= 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS transacciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(36) UNIQUE,
    cuenta_origen VARCHAR(20),
    cuenta_destino VARCHAR(20),
    monto DECIMAL(12, 2) NOT NULL,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estado VARCHAR(20) NOT NULL DEFAULT 'PENDIENTE',
    nodo_maestro VARCHAR(255),
    FOREIGN KEY (cuenta_origen) REFERENCES cuentas(numero_cuenta),
    FOREIGN KEY (cuenta_destino) REFERENCES cuentas(numero_cuenta)
) ENGINE=InnoDB;

-- Datos de prueba (idempotentes)
INSERT INTO cuentas (numero_cuenta, cliente, saldo) VALUES ('1111', 'Juan Perez', 1000.00)
  ON DUPLICATE KEY UPDATE cliente = VALUES(cliente);
INSERT INTO cuentas (numero_cuenta, cliente, saldo) VALUES ('2222', 'Maria Gomez', 500.00)
  ON DUPLICATE KEY UPDATE cliente = VALUES(cliente);
INSERT INTO cuentas (numero_cuenta, cliente, saldo) VALUES ('3333', 'Carlos Lopez', 750.00)
  ON DUPLICATE KEY UPDATE cliente = VALUES(cliente);
INSERT INTO cuentas (numero_cuenta, cliente, saldo) VALUES ('4444', 'Ana Torres', 2000.00)
  ON DUPLICATE KEY UPDATE cliente = VALUES(cliente);
