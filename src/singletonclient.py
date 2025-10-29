# ============================================
# Programa: singletonclient.py
# Autor: Camilo Escar
# Version: 1.0
# Descripcion: Cliente para acciones get/set/list
#              hacia el servidor singletonproxyobserver.py
# ============================================

import socket
import json
import argparse
import uuid
import logging
from pathlib import Path

# --------------------------------------------
# CONFIGURACIoN
# --------------------------------------------
HOST = "localhost"
PORT = 8080
BUFFER_TAM = 4096

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# --------------------------------------------
# FUNCIONES AUXILIARES
# --------------------------------------------
def envio_request(action_data, host, port):
    """Envia un JSON al servidor y recibe respuesta."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(json.dumps(action_data).encode('utf-8'))
        data = s.recv(BUFFER_TAM)
    return data.decode('utf-8')


# --------------------------------------------
# MAIN
# --------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Cliente Singleton para acciones get/set/list")
    parser.add_argument("-i", "--input", required=True, help="Archivo JSON de entrada")
    parser.add_argument("-o", "--output", help="Archivo JSON de salida (opcional)")
    parser.add_argument("-s", "--server", default=HOST, help="Host del servidor (default localhost)")
    parser.add_argument("-p", "--port", type=int, default=PORT, help="Puerto del servidor (default 8080)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Modo verboso")

    args = parser.parse_args()

    # Configurar logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Leer input JSON
    input_path = Path(args.input)
    if not input_path.exists():
        logging.error(f"No se encuentra el archivo {args.input}")
        return

    with open(input_path, "r") as f:
        data = json.load(f)

    # Agregar UUID si no está
    if "UUID" not in data:
        data["UUID"] = str(uuid.getnode())
        
    if "ACTION" not in data or not str(data["ACTION"]).strip():
        logging.error("El campo 'ACTION' es obligatorio en el JSON de entrada.")
        exit(1)  #  Devolver código de error distinto de cero ya que falta el campo ACTION

    logging.info(f"Enviando accion: {data.get('ACTION')} al servidor {args.server}:{args.port}")
    response = envio_request(data, args.server, args.port)

    # Mostrar o guardar salida
    if args.output:
        with open(args.output, "w") as f:
            f.write(response)
        logging.info(f"Salida guardada en {args.output}")
    else:
        print(response)


if __name__ == "__main__":
    main()
