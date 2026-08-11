#!/bin/bash
# Archivo: scripts/open-firewall.sh
# Proposito: Abrir los puertos del Firewall de Linux/macOS para el Clúster Raft
# Instrucciones: Ejecutar este script desde Terminal usando sudo: sudo bash scripts/open-firewall.sh

echo "======================================================="
echo " Configurando Firewall para Examen Naranjo (Linux/Mac) "
echo "======================================================="
echo ""

if [ "$EUID" -ne 0 ]; then 
    echo "ERROR: Este script necesita permisos de root."
    echo "Por favor, ejecutalo usando sudo: sudo bash scripts/open-firewall.sh"
    exit 1
fi

OS="$(uname -s)"

if [ "$OS" = "Darwin" ]; then
    echo "Detectado macOS..."
    echo "Nota: macOS normalmente te pregunta con una ventana emergente si deseas permitir conexiones entrantes a Docker."
    echo "Si bloqueaste accidentalmente los puertos, puedes desactivar temporalmente el firewall con:"
    echo "sudo /usr/libexec/ApplicationFirewall/socketfilterfw --setglobalstate off"
    echo "O agregar docker: sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /Applications/Docker.app/Contents/MacOS/Docker"
    
elif command -v ufw > /dev/null; then
    echo "Detectado UFW (Ubuntu/Debian)..."
    sudo ufw allow 3306/tcp
    sudo ufw allow 2379/tcp
    sudo ufw allow 2380/tcp
    sudo ufw reload
    
elif command -v firewall-cmd > /dev/null; then
    echo "Detectado firewalld (CentOS/Fedora/RHEL)..."
    sudo firewall-cmd --add-port=3306/tcp --permanent
    sudo firewall-cmd --add-port=2379/tcp --permanent
    sudo firewall-cmd --add-port=2380/tcp --permanent
    sudo firewall-cmd --reload
    
elif command -v iptables > /dev/null; then
    echo "Usando iptables generico..."
    sudo iptables -A INPUT -p tcp --dport 3306 -j ACCEPT
    sudo iptables -A INPUT -p tcp --dport 2379 -j ACCEPT
    sudo iptables -A INPUT -p tcp --dport 2380 -j ACCEPT
    
else
    echo "No se encontro un firewall conocido (UFW, firewalld, iptables). Por favor configura manualmente los puertos 3306, 2379 y 2380."
    exit 1
fi

echo ""
echo "¡Puertos 3306, 2379 y 2380 configurados exitosamente!"
echo "Ahora tus companeros deberian poder conectarse a tus contenedores."
echo "======================================================="
