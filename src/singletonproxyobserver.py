# ============================================
# Programa: singletonproxyobserver.py
# Versión: 1.3
# Autor: Camilo Escar
# Materia: Ingeniería de Software II - UADER-FCyT
# Descripción: Servidor TCP con patrones Singleton, Proxy y Observer
#              para gestionar CorporateData y CorporateLog en AWS DynamoDB
# ============================================

import socket
import threading
import json
import boto3
import datetime
import logging
from uuid import uuid4
from decimal import Decimal
from botocore.exceptions import ClientError

# --------------------------------------------
# CONFIGURACIÓN GLOBAL
# --------------------------------------------
HOST = "localhost"
PORT = 8080

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ===========================================================
# UTILIDADES
# ===========================================================
def clean_dict(data):
    """Limpia valores vacíos o nulos y convierte tipos compatibles."""
    clean = {}
    for key, value in data.items():
        if value is None or str(value).strip() == "":
            continue
        if isinstance(value, float):
            clean[key] = Decimal(str(value))
        else:
            clean[key] = str(value).strip()
    return clean


# ===========================================================
# PATRÓN SINGLETON
# ===========================================================
class SingletonMeta(type):
    """Metaclase para asegurar una única instancia (Singleton)."""
    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                logging.debug(f"Creando instancia única de {cls.__name__}")
                cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


# ===========================================================
# CORPORATE DATA
# ===========================================================
class CorporateData(metaclass=SingletonMeta):
    """Acceso Singleton a la tabla CorporateData."""
    def __init__(self):
        self.table = boto3.resource('dynamodb', region_name='us-east-1').Table('CorporateData')
        logging.info("Singleton CorporateData inicializado")

    def get_item(self, item_id):
        if not item_id:
            return {"Error": "ID no puede estar vacío"}
        try:
            logging.debug(f"Obteniendo item con id={item_id}")
            response = self.table.get_item(Key={'id': str(item_id)})
            return response.get('Item', {"Error": "No encontrado"})
        except ClientError as e:
            logging.error(f"Error en get_item: {e}")
            return {"Error": str(e)}

    def list_items(self):
        try:
            logging.debug("Listando todos los items de CorporateData")
            return self.table.scan().get('Items', [])
        except ClientError as e:
            logging.error(f"Error en list_items: {e}")
            return {"Error": str(e)}

    def set_item(self, data):
        """Inserta o actualiza un registro en la tabla CorporateData."""
        if "id" not in data or not str(data["id"]).strip():
            return {"Error": "El campo 'id' es obligatorio y no puede estar vacío"}
        try:
            clean_data = clean_dict(data)
            logging.info(f"Guardando en DynamoDB:\n{json.dumps(clean_data, indent=2, default=str)}")
            self.table.put_item(Item=clean_data)
            logging.info(f"Registro guardado exitosamente con id: {clean_data['id']}")
            return clean_data
        except Exception as e:
            logging.error(f"Error guardando en CorporateData: {e}")
            return {"Error": str(e)}


# ===========================================================
# CORPORATE LOG
# ===========================================================
class CorporateLog(metaclass=SingletonMeta):
    """Acceso Singleton a la tabla CorporateLog."""
    def __init__(self):
        self.table = boto3.resource('dynamodb', region_name='us-east-1').Table('CorporateLog')
        logging.info("Singleton CorporateLog inicializado")

    def log_action(self, client_uuid, action, record_id=None):
        """Registra una acción en CorporateLog."""
        timestamp = datetime.datetime.now().isoformat()
        try:
            response = self.table.get_item(Key={'id': str(client_uuid)})
            existing = response.get('Item', {})
            history = existing.get('history', [])

            history.append({
                "action": str(action),
                "timestamp": timestamp,
                "record_id": str(record_id) if record_id else "N/A"
            })

            entry = {
                "id": str(client_uuid),
                "last_action": str(action),
                "last_timestamp": timestamp,
                "last_record_id": str(record_id) if record_id else "N/A",
                "history": history
            }

            self.table.put_item(Item=entry)
            logging.info(f"Log registrado: {action} por {client_uuid}")
        except Exception as e:
            logging.error(f"Error al registrar log: {e}")


# ===========================================================
# OBSERVER MANAGER
# ===========================================================
class ObserverManager:
    """Gestiona las suscripciones de clientes."""
    def __init__(self):
        self.subscribers = []
        self.lock = threading.Lock()

    def add_subscriber(self, conn, addr):
        with self.lock:
            self.subscribers.append((conn, addr))
            logging.info(f"Nuevo suscriptor: {addr} (Total={len(self.subscribers)})")

    def remove_subscriber(self, conn):
        with self.lock:
            self.subscribers = [(c, a) for c, a in self.subscribers if c != conn]
            logging.info(f"Suscriptor eliminado. Total actual={len(self.subscribers)}")

    def notify_all(self, message):
        """Envía el mensaje a todos los suscriptores."""
        with self.lock:
            for conn, addr in list(self.subscribers):
                try:
                    conn.sendall(json.dumps(message, default=str).encode('utf-8'))
                    logging.info(f"Notificación enviada a {addr}")
                except Exception as e:
                    logging.warning(f"No se pudo notificar a {addr}: {e}")
                    self.remove_subscriber(conn)


# ===========================================================
# SERVIDOR (PROXY)
# ===========================================================
class ProxyServer:
    """Servidor TCP con manejo de acciones (SET, GET, LIST, SUBSCRIBE)."""
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.data = CorporateData()
        self.log = CorporateLog()
        self.observer = ObserverManager()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        logging.info(f"Servidor escuchando en {self.host}:{self.port}")

    def start(self):
        try:
            while True:
                conn, addr = self.sock.accept()
                logging.info(f"Conexión aceptada desde {addr}")
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Servidor detenido manualmente")
        finally:
            self.sock.close()

    # ------------------------------------
    # CLIENT HANDLER
    # ------------------------------------
    def handle_client(self, conn, addr):
        try:
            data = self._recv_all(conn)
            if not data:
                logging.warning(f"Conexión vacía desde {addr}")
                return

            logging.debug(f"Datos crudos recibidos de {addr}: {data[:300]}...")
            request = json.loads(data)
            action = request.get("ACTION", "").lower()
            uuid_client = request.get("UUID", str(uuid4()))

            logging.info(f"Acción recibida: {action.upper()} desde {addr}")

            # Ruteo de acciones
            if action == "set":
                self._action_set(conn, request, uuid_client)
            elif action == "get":
                self._action_get(conn, request, uuid_client)
            elif action == "list":
                self._action_list(conn, uuid_client)
            elif action == "subscribe":
                self._action_subscribe(conn, addr, request, uuid_client)
            else:
                self._send_json(conn, {"Error": f"Acción no reconocida: {action}"})

        except json.JSONDecodeError as e:
            self._send_json(conn, {"Error": f"JSON inválido: {str(e)}"})
        except Exception as e:
            logging.exception(f"Error manejando cliente {addr}: {e}")
            self._send_json(conn, {"Error": str(e)})
        finally:
            try:
                conn.close()
                logging.debug(f"Conexión cerrada con {addr}")
            except:
                pass

    # ------------------------------------
    # ACCIONES
    # ------------------------------------
    def _action_set(self, conn, req, uuid_client):
        data = {k: v for k, v in req.items() if k.upper() not in ["UUID", "ACTION"]}
        self.log.log_action(uuid_client, "set", data.get("id"))
        result = self.data.set_item(data)
        self._send_json(conn, result)
        if "Error" not in result:
            self.observer.notify_all(result)

    def _action_get(self, conn, req, uuid_client):
        item_id = req.get("id") or req.get("ID")
        self.log.log_action(uuid_client, "get", item_id)
        result = self.data.get_item(item_id)
        self._send_json(conn, result)

    def _action_list(self, conn, uuid_client):
        self.log.log_action(uuid_client, "list")
        result = self.data.list_items()
        self._send_json(conn, result)

    def _action_subscribe(self, conn, addr, req, uuid_client):
        self.log.log_action(uuid_client, "subscribe", req.get("id"))
        self.observer.add_subscriber(conn, addr)
        self._send_json(conn, {"status": "subscribed", "message": "Suscripción exitosa"})
        try:
            while conn.recv(1):  # Escucha pasivamente hasta desconexión
                pass
        except Exception:
            logging.info(f"Cliente suscrito {addr} desconectado.")
        finally:
            self.observer.remove_subscriber(conn)

    # ------------------------------------
    # AUXILIARES
    # ------------------------------------
    def _recv_all(self, conn):
        data_chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data_chunks.append(chunk)
            if len(chunk) < 4096:
                break
        return b''.join(data_chunks).decode('utf-8') if data_chunks else None

    def _send_json(self, conn, obj):
        conn.sendall(json.dumps(obj, default=str).encode('utf-8'))


# ===========================================================
# MAIN
# ===========================================================
if __name__ == "__main__":
    ProxyServer().start()
