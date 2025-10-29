# ============================================
# Programa: observerclient.py (version mejorada)
# Autor: Camilo Escar / Ajustado por GPT-5
# Version: 1.1
# Descripcion: Cliente observador con reconexion controlada
# ============================================

import socket
import json
import argparse
import uuid
import time
import logging

# --------------------------------------------
# CONFIGURACIoN
# --------------------------------------------
REINTENTOS = 30     # segundos entre reintentos de conexion
BUFFER_TAM = 4096

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def conexion_suscripcion(host, port, uuid_client, output_file):
    ultimo_dato = None

    while True:
        try:
            logging.info(f"Intentando conectar con {host}:{port}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)  # evita bloqueo infinito al conectar
            s.connect((host, port))

            msg = {"UUID": uuid_client, "ACTION": "subscribe"}
            s.sendall(json.dumps(msg).encode('utf-8'))
            logging.info(f"Suscrito al servidor {host}:{port}. Esperando actualizaciones...")

            s.settimeout(None)  # conexion establecida, sin timeout
            while True:
                data = s.recv(BUFFER_TAM)
                if not data:
                    logging.warning("Conexion cerrada por el servidor.")
                    break

                decoded = data.decode('utf-8').strip()
                if decoded and decoded != ultimo_dato:
                    print("\nActualizacion recibida:\n", decoded)
                    try:
                        parsed = json.loads(decoded)
                        print(json.dumps(parsed, indent=2, ensure_ascii=False))
                    except json.JSONDecodeError:
                        print(decoded)

                    if output_file:
                        with open(output_file, "a") as f:
                            f.write(decoded + "\n")

                    ultimo_dato = decoded
                else:
                    logging.debug("Actualizacion repetida ignorada")

        except (ConnectionRefusedError, TimeoutError) as e:
            logging.warning(f"No se pudo conectar al servidor ({e}). Reintentando en {REINTENTOS}s...")
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
        finally:
            try:
                s.close()
            except:
                pass
            logging.info(f"Reintentando conexion en {REINTENTOS}s...\n")
            time.sleep(REINTENTOS)


def main():
    parser = argparse.ArgumentParser(description="Cliente observador (subscribe)")
    parser.add_argument("-s", "--server", default="localhost", help="Host del servidor (default localhost)")
    parser.add_argument("-p", "--port", type=int, default=8080, help="Puerto del servidor (default 8080)")
    parser.add_argument("-o", "--output", help="Archivo para guardar actualizaciones")
    parser.add_argument("-v", "--verbose", action="store_true", help="Modo verboso")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    uuid_client = str(uuid.getnode())
    conexion_suscripcion(args.server, args.port, uuid_client, args.output)


if __name__ == "__main__":
    main()
