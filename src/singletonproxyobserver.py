# ============================================
# Programa: singletonproxyobserver.py
# Versión: 1.2
# Autor: Camilo Escar
# Materia: Ingeniería de Software II - UADER-FCyT
# Descripción: Servidor TCP que implementa los patrones
#              Singleton, Proxy y Observer para la gestión
#              de CorporateData y CorporateLog en AWS DynamoDB
# ============================================

import socket
import threading
import json
import boto3
import uuid
import datetime
import logging
from botocore.exceptions import ClientError
from decimal import Decimal

# --------------------------------------------
# CONFIGURACIÓN GLOBAL
# --------------------------------------------
HOST = "localhost"
PORT = 8080
DEBUG = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ===========================================================
# UTILIDADES
# ===========================================================
def convert_to_dynamodb_format(data):
    """
    Convierte los datos al formato compatible con DynamoDB.
    DynamoDB no acepta floats, deben ser Decimal.
    """
    if isinstance(data, dict):
        return {k: convert_to_dynamodb_format(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_to_dynamodb_format(item) for item in data]
    elif isinstance(data, float):
        return Decimal(str(data))
    else:
        return data


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
                cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class CorporateDataSingleton(metaclass=SingletonMeta):
    """Acceso Singleton a la tabla CorporateData."""
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table('CorporateData')
        logging.info("CorporateDataSingleton inicializado")

    def get_item(self, item_id):
        try:
            if not item_id:
                return {'Error': 'ID no puede estar vacío'}
            
            response = self.table.get_item(Key={'id': str(item_id)})
            return response.get('Item', {'Error': 'No encontrado'})
        except ClientError as e:
            logging.error(f"Error en get_item: {e}")
            return {'Error': str(e)}

    def list_items(self):
        try:
            response = self.table.scan()
            return response.get('Items', [])
        except ClientError as e:
            logging.error(f"Error en list_items: {e}")
            return {'Error': str(e)}

    def set_item(self, data):
        try:
            if 'id' not in data:
                error_msg = "El campo 'id' es obligatorio"
                logging.error(error_msg)
                return {'Error': error_msg}
            
            if not data['id'] or str(data['id']).strip() == '':
                error_msg = "El campo 'id' no puede estar vacío"
                logging.error(error_msg)
                return {'Error': error_msg}
            
            clean_data = {}
            for key, value in data.items():
                if value is not None and str(value).strip() != '':
                    # Asegurar que todos los valores sean strings o tipos compatibles
                    if isinstance(value, (int, float)):
                        clean_data[key] = Decimal(str(value)) if isinstance(value, float) else value
                    else:
                        clean_data[key] = str(value).strip()
            
            if 'id' not in clean_data:
                error_msg = "El campo 'id' se perdió durante el procesamiento"
                logging.error(error_msg)
                return {'Error': error_msg}
            
            logging.info(f"Guardando en DynamoDB: {json.dumps(clean_data, indent=2, default=str)}")
            
            self.table.put_item(Item=clean_data)
            
            logging.info(f"✅ Registro guardado exitosamente con id: {clean_data['id']}")
            return clean_data
            
        except ClientError as e:
            error_msg = f"Error de DynamoDB: {str(e)}"
            logging.error(error_msg)
            return {'Error': error_msg}
        except Exception as e:
            error_msg = f"Error inesperado en set_item: {str(e)}"
            logging.error(error_msg)
            return {'Error': error_msg}


class CorporateLogSingleton(metaclass=SingletonMeta):
    """Acceso Singleton a la tabla CorporateLog."""
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table('CorporateLog')
        logging.info("CorporateLogSingleton inicializado")

    def log_action(self, uuid_client, action, record_id=None):
        """
        Registra una acción en CorporateLog.
        Usa el UUID del cliente como clave primaria y guarda el historial en una lista.
        """
        try:
            timestamp = datetime.datetime.now().isoformat()
            
            # Obtener log existente del cliente
            try:
                response = self.table.get_item(Key={'id': str(uuid_client)})
                existing_log = response.get('Item', {})
                history = existing_log.get('history', [])
            except:
                history = []
            
            # Agregar nueva entrada al historial
            history.append({
                'action': str(action),
                'timestamp': timestamp,
                'record_id': str(record_id) if record_id else 'N/A'
            })
            
            # Guardar log actualizado
            entry = {
                'id': str(uuid_client),
                'last_action': str(action),
                'last_timestamp': timestamp,
                'last_record_id': str(record_id) if record_id else 'N/A',
                'history': history
            }
            
            self.table.put_item(Item=entry)
            logging.info(f"Log registrado: {action} por {uuid_client}")
        except Exception as e:
            logging.error(f"Error al registrar log: {e}")


# ===========================================================
# PATRÓN OBSERVER
# ===========================================================
class ObserverManager:
    """Gestiona las suscripciones y notificaciones."""
    def __init__(self):
        self.subscribers = []  # lista de sockets conectados
        self.lock = threading.Lock()

    def add_subscriber(self, conn, addr):
        with self.lock:
            self.subscribers.append((conn, addr))
            logging.info(f"Nuevo suscriptor agregado: {addr} (Total: {len(self.subscribers)})")

    def remove_subscriber(self, conn):
        with self.lock:
            original_count = len(self.subscribers)
            self.subscribers = [(c, a) for c, a in self.subscribers if c != conn]
            if len(self.subscribers) < original_count:
                logging.info(f"Suscriptor eliminado. (Total: {len(self.subscribers)})")

    def notify_all(self, message):
        """Envía el mensaje JSON a todos los clientes suscritos."""
        with self.lock:
            disconnected = []
            for conn, addr in self.subscribers:
                try:
                    conn.sendall(json.dumps(message, default=str).encode('utf-8'))
                    logging.debug(f"Notificación enviada a {addr}")
                except Exception as e:
                    logging.warning(f"No se pudo notificar a {addr}: {e}")
                    disconnected.append(conn)
            
            # Remover conexiones fallidas
            for conn in disconnected:
                self.remove_subscriber(conn)


# ===========================================================
# PATRÓN PROXY (Servidor principal)
# ===========================================================
class ProxyServer:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.observer = ObserverManager()
        self.data = CorporateDataSingleton()
        self.log = CorporateLogSingleton()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        logging.info(f"Servidor escuchando en {self.host}:{self.port}")

    def start(self):
        """Inicia el servidor y acepta conexiones."""
        try:
            while True:
                conn, addr = self.sock.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Servidor detenido manualmente.")
        finally:
            self.sock.close()

    def handle_client(self, conn, addr):
        """Maneja cada cliente conectado."""
        try:
            data_chunks = []
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data_chunks.append(chunk)
                # Si el chunk es menor que 4096, asumimos que es el último
                if len(chunk) < 4096:
                    break
            
            if not data_chunks:
                logging.warning(f"Conexión vacia desde {addr}")
                return

            full_data = b''.join(data_chunks).decode('utf-8')
            logging.debug(f"Datos recibidos de {addr}: {full_data[:200]}...")
            
            request = json.loads(full_data)
            action = request.get("ACTION", "").lower()
            uuid_client = request.get("UUID", "Desconocido")

            logging.info(f"Acción recibida: {action} desde {addr}")

            if action == "set":
                data_to_save = {k: v for k, v in request.items() 
                               if k.upper() not in ["UUID", "ACTION"]}
                
                logging.info(f"Procesando SET con datos: {json.dumps(data_to_save, indent=2)}")
                
                if "id" not in data_to_save:
                    error_msg = {"Error": "Falta el campo obligatorio 'id' en el registro"}
                    logging.error(error_msg["Error"])
                    conn.sendall(json.dumps(error_msg).encode('utf-8'))
                    conn.close()
                    return
                
                # Registrar en log
                self.log.log_action(uuid_client, action, data_to_save.get("id"))
                
                # Guardar en DynamoDB
                result = self.data.set_item(data_to_save)
                
                # Enviar respuesta al cliente
                conn.sendall(json.dumps(result, default=str).encode('utf-8'))
                
                if "Error" not in result:
                    self.observer.notify_all(result)
                    logging.info(f"Observadores notificados del cambio en id: {data_to_save.get('id')}")
            
            elif action == "get":
                item_id = request.get("id") or request.get("ID")
                self.log.log_action(uuid_client, action, item_id)
                result = self.data.get_item(item_id)
                conn.sendall(json.dumps(result, default=str).encode('utf-8'))

            # --- Manejo de acciones ---
            elif action == "subscribe":
                record_id = request.get("id") or request.get("ID")
                # Registrar la acción con id
                self.log.log_action(uuid_client, action, record_id)
                logging.info(f"Log registrado: {action} por {uuid_client}")
                
                self.observer.add_subscriber(conn, addr)
                response = {"status": "subscribed", "message": "Suscripcion exitosa"}
                conn.sendall(json.dumps(response).encode('utf-8'))

                # Escuchar pasivamente para detectar desconexión
                try:
                    while True:
                        ping = conn.recv(1)
                        if not ping:
                            logging.info(f"Suscriptor {addr} se desconectó.")
                            break
                except Exception as e:
                    logging.debug(f"Fin de conexión con {addr}: {e}")
                finally:
                    self.observer.remove_subscriber(conn)
                    conn.close()
                return


            

            elif action == "list":
                self.log.log_action(uuid_client, action)
                result = self.data.list_items()
                conn.sendall(json.dumps(result, default=str).encode('utf-8'))

            
            else:
                error_msg = {"Error": f"Accion no reconocida: {action}"}
                logging.warning(error_msg["Error"])
                conn.sendall(json.dumps(error_msg).encode('utf-8'))

        except json.JSONDecodeError as e:
            error_msg = {"Error": f"JSON inválido: {str(e)}"}
            logging.error(f"Error de JSON desde {addr}: {e}")
            try:
                conn.sendall(json.dumps(error_msg).encode('utf-8'))
            except:
                pass
        except Exception as e:
            error_msg = {"Error": f"Error del servidor: {str(e)}"}
            logging.error(f"Error manejando cliente {addr}: {e}", exc_info=True)
            try:
                conn.sendall(json.dumps(error_msg).encode('utf-8'))
            except:
                pass
        finally:
            try:
                conn.close()
            except:
                pass


# ===========================================================
# MAIN
# ===========================================================
if __name__ == "__main__":
    server = ProxyServer()
    server.start()
