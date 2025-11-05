# ============================================
# Programa: singletonproxyobserver.py
# Version: 1.0
# Autor: Camilo Escar
# Materia: Ingenieria de Software II - UADER-FCyT
# Descripcion: Servidor TCP con patrones Singleton, Proxy y Observer
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
# CONFIGURACIoN GLOBAL
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
def limpiar_dicc(data):
    """Limpia valores vacios o nulos y convierte tipos compatibles."""
    dato = {}
    for key, value in data.items():
        if value is None or str(value).strip() == "":
            continue
        if isinstance(value, float):
            dato[key] = Decimal(str(value))
        else:
            dato[key] = str(value).strip()
    return dato


# ===========================================================
# PATRoN SINGLETON
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
            return {"Error": "ID no puede estar vacio"}
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
            return {"Error": "El campo 'id' es obligatorio y no puede estar vacio"}
        try:
            dato_limpio = limpiar_dicc(data)
            logging.info(f"Guardando en DynamoDB:\n{json.dumps(dato_limpio, indent=2, default=str)}")
            self.table.put_item(Item=dato_limpio)
            logging.info(f"Registro guardado exitosamente con id: {dato_limpio['id']}")
            return dato_limpio
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

    def registro(self, client_uuid, action, record_id=None):
        """Registra una accion en CorporateLog."""
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

    def get_log(self, client_uuid):
        """Obtiene el log completo de un cliente específico."""
        if not client_uuid:
            return {"Error": "UUID no puede estar vacio"}
        try:
            logging.debug(f"Obteniendo log para UUID={client_uuid}")
            response = self.table.get_item(Key={'id': str(client_uuid)})
            return response.get('Item', {"Error": "Log no encontrado para este UUID"})
        except ClientError as e:
            logging.error(f"Error en get_log: {e}")
            return {"Error": str(e)}

    def list_logs(self):
        """Lista todos los registros de auditoría."""
        try:
            logging.debug("Listando todos los logs de CorporateLog")
            response = self.table.scan()
            items = response.get('Items', [])
            
            # Ordenar por último timestamp (más reciente primero)
            items.sort(key=lambda x: x.get('last_timestamp', ''), reverse=True)
            
            return items
        except ClientError as e:
            logging.error(f"Error en list_logs: {e}")
            return {"Error": str(e)}


# ===========================================================
# OBSERVER MANAGER
# ===========================================================
class ObserverSuscriptor:
    """Gestiona las suscripciones de clientes."""
    def __init__(self):
        self.suscriptores = []
        self.lock = threading.Lock()

    def agregar_suscriptor(self, conn, addr):
        with self.lock:
            self.suscriptores.append((conn, addr))
            logging.info(f"Nuevo suscriptor: {addr} (Total={len(self.suscriptores)})")

    def remover_suscriptor(self, conn):
        with self.lock:
            self.suscriptores = [(c, a) for c, a in self.suscriptores if c != conn]
            logging.info(f"Suscriptor eliminado. Total actual={len(self.suscriptores)}")

    def notificar(self, message):
        """Envia el mensaje a todos los suscriptores."""
        with self.lock:
            for conn, addr in list(self.suscriptores):
                try:
                    conn.sendall(json.dumps(message, default=str).encode('utf-8'))
                    logging.info(f"Notificacion enviada a {addr}")
                except Exception as e:
                    logging.warning(f"No se pudo notificar a {addr}: {e}")
                    self.remover_suscriptor(conn)


# ===========================================================
# SERVIDOR (PROXY)
# ===========================================================
class ServidorProxy:
    """Servidor TCP con manejo de acciones (SET, GET, LIST, LISTLOG, GETLOG, SUBSCRIBE)."""
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.data = CorporateData()
        self.log = CorporateLog()
        self.observer = ObserverSuscriptor()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind((self.host, self.port))
        except OSError as e:
            if e.errno == 98: 
                logging.error(f"El servidor ya está en ejecución en {self.host}:{self.port}")
                print(f"\n[ERROR] Ya hay un servidor corriendo en {self.host}:{self.port}\n")
                exit(1)
            else:
                raise
        self.sock.listen(5)
        logging.info(f"Servidor escuchando en {self.host}:{self.port}")

    def start(self):
        try:
            while True:
                conn, addr = self.sock.accept()
                logging.info(f"Conexion aceptada desde {addr}")
                threading.Thread(target=self.handle_cliente, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Servidor detenido manualmente")
        finally:
            self.sock.close()

    # ------------------------------------
    # CLIENT HANDLER
    # ------------------------------------
    def handle_cliente(self, conn, addr):
        try:
            data = self._recibir_datos(conn)
            if not data:
                logging.warning(f"Conexion vacia desde {addr}")
                return

            logging.debug(f"Datos crudos recibidos de {addr}: {data[:300]}...")
            request = json.loads(data)
            action = request.get("ACTION", "").lower()
            uuid_client = request.get("UUID", str(uuid4()))

            logging.info(f"Accion recibida: {action.upper()} desde {addr}")

            # Ruteo de acciones
            if action == "set":
                self._accion_set(conn, request, uuid_client)
            elif action == "get":
                self._accion_get(conn, request, uuid_client)
            elif action == "list":
                self._accion_list(conn, uuid_client)
            elif action == "listlog":
                self._accion_listlog(conn, uuid_client)
            elif action == "getlog":
                self._accion_getlog(conn, request, uuid_client)
            elif action == "subscribe":
                self._accion_subscribe(conn, addr, request, uuid_client)
            else:
                self._enviar_json(conn, {"Error": f"Accion no reconocida: {action}"})

        except json.JSONDecodeError as e:
            self._enviar_json(conn, {"Error": f"JSON inválido: {str(e)}"})
        except Exception as e:
            logging.exception(f"Error manejando cliente {addr}: {e}")
            self._enviar_json(conn, {"Error": str(e)})
        finally:
            try:
                conn.close()
                logging.debug(f"Conexion cerrada con {addr}")
            except:
                pass

    # ------------------------------------
    # ACCIONES
    # ------------------------------------
    def _accion_set(self, conn, req, uuid_client):
        data = {k: v for k, v in req.items() if k.upper() not in ["UUID", "ACTION"]}
        self.log.registro(uuid_client, "set", data.get("id"))
        result = self.data.set_item(data)
        self._enviar_json(conn, result)
        if "Error" not in result:
            self.observer.notificar(result)

    def _accion_get(self, conn, req, uuid_client):
        item_id = req.get("id") or req.get("ID")
        self.log.registro(uuid_client, "get", item_id)
        result = self.data.get_item(item_id)
        self._enviar_json(conn, result)

    def _accion_list(self, conn, uuid_client):
        self.log.registro(uuid_client, "list")
        result = self.data.list_items()
        self._enviar_json(conn, result)

    def _accion_listlog(self, conn, uuid_client):
        """Lista todos los registros de auditoría."""
        self.log.registro(uuid_client, "listlog")
        result = self.log.list_logs()
        self._enviar_json(conn, result)

    def _accion_getlog(self, conn, req, uuid_client):
        """Obtiene el log de un UUID específico."""
        target_uuid = req.get("target_uuid") or req.get("TARGET_UUID")
        if not target_uuid:
            self._enviar_json(conn, {"Error": "Se requiere 'target_uuid' para obtener un log específico"})
            return
        
        self.log.registro(uuid_client, "getlog", target_uuid)
        result = self.log.get_log(target_uuid)
        self._enviar_json(conn, result)

    def _accion_subscribe(self, conn, addr, req, uuid_client):
        self.log.registro(uuid_client, "subscribe", req.get("id"))
        self.observer.agregar_suscriptor(conn, addr)
        self._enviar_json(conn, {"status": "subscribed", "message": "Suscripcion exitosa"})
        try:
            while conn.recv(1):
                pass
        except Exception:
            logging.info(f"Cliente suscrito {addr} desconectado.")
        finally:
            self.observer.remover_suscriptor(conn)

    # ------------------------------------
    # AUXILIARES
    # ------------------------------------
    def _recibir_datos(self, conn):
        data_chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data_chunks.append(chunk)
            if len(chunk) < 4096:
                break
        return b''.join(data_chunks).decode('utf-8') if data_chunks else None

    def _enviar_json(self, conn, obj):
        conn.sendall(json.dumps(obj, default=str).encode('utf-8'))


# ===========================================================
# MAIN
# ===========================================================
if __name__ == "__main__":
    ServidorProxy().start()