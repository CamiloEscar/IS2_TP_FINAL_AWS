import subprocess
import time
import json
import os
import boto3
from boto3.dynamodb.conditions import Attr
import threading
import sys

def monitor_process_output(process, prefix="[SERVER]"):
    """Lee y muestra la salida del servidor en tiempo real"""
    def read_output(pipe, prefix):
        try:
            for line in iter(pipe.readline, ''):
                if line:
                    print(f"{prefix} {line.rstrip()}")
                    sys.stdout.flush()
        except:
            pass
    
    if process.stdout:
        threading.Thread(target=read_output, args=(process.stdout, f"{prefix} OUT"), daemon=True).start()
    if process.stderr:
        threading.Thread(target=read_output, args=(process.stderr, f"{prefix} ERR"), daemon=True).start()

# --- Espera hasta que el registro aparezca en DynamoDB ---
def wait_for_log(uuid, action, table, timeout=15):
    """
    Busca un log por UUID del cliente y verifica la última acción.
    """
    start = time.time()
    attempt = 0
    while time.time() - start < timeout:
        attempt += 1
        try:
            # Buscar directamente por el UUID como clave
            response = table.get_item(Key={'id': str(uuid)})
            item = response.get('Item')
            
            # Debug
            if attempt == 1 or attempt % 5 == 0:
                print(f"[DEBUG] Intento {attempt}: Buscando id={uuid}, action={action}")
                if item:
                    print(f"[DEBUG] Item encontrado: last_action={item.get('last_action')}")
                else:
                    print(f"[DEBUG] No existe log para UUID={uuid}")
            
            # Verificar si la última acción coincide
            if item and item.get('last_action') == action:
                print(f"[DEBUG] ✓ Log encontrado en intento {attempt}")
                return item
                
        except Exception as e:
            print(f"[ERROR] Error al buscar en DynamoDB: {e}")
            
        time.sleep(0.5)
    
    print(f"[DEBUG] ✗ No se encontró log después de {attempt} intentos")
    return None

def test_happy_path_all_actions():
    BASE_DIR = os.path.abspath("src")
    INPUT_SET_PATH = os.path.join(BASE_DIR, "input_set.json")
    OUT_SET_PATH = os.path.join(BASE_DIR, "out_set.json")
    INPUT_GET_PATH = os.path.join(BASE_DIR, "input_get.json")
    OUT_GET_PATH = os.path.join(BASE_DIR, "out_get.json")
    INPUT_SUB_PATH = os.path.join(BASE_DIR, "input_sub.json")

    # --- Conexión a DynamoDB ---
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    log_table = dynamodb.Table('CorporateLog')

    # --- Levantar servidor ---
    print("[TEST] Levantando servidor...")
    server = subprocess.Popen(
        ["python", "singletonproxyobserver.py"],
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    # Monitorear salida del servidor en tiempo real
    monitor_process_output(server)
    
    time.sleep(3)  # Aumentado a 3 segundos para dar más tiempo

    try:
        # --- ACCIÓN SET ---
        print("\n[TEST] === PRUEBA SET ===")
        with open(INPUT_SET_PATH) as f:
            input_set = json.load(f)
        
        print(f"[TEST] UUID a usar: {input_set['UUID']}")
        print(f"[TEST] Acción: SET")

        subprocess.run(
            ["python", "singletonclient.py", f"-i={INPUT_SET_PATH}", f"-o={OUT_SET_PATH}", "-v"],
            cwd=BASE_DIR,
            check=True
        )
        assert os.path.exists(OUT_SET_PATH), "El archivo de salida SET no se generó"
        
        print("[TEST] Esperando registro en DynamoDB...")
        time.sleep(1)  # Dar un segundo extra antes de buscar

        # --- Verificar log SET en DynamoDB ---
        row = wait_for_log(input_set["UUID"], "set", log_table)
        assert row is not None, "No se registró la acción SET en CorporateLog"
        print(f"[TEST] Log SET registrado correctamente")
        print(f"[TEST] Detalles: {json.dumps(row, indent=2, default=str)}")

        # --- ACCIÓN GET ---
        print("\n[TEST] === PRUEBA GET ===")
        input_get = {"UUID": input_set["UUID"], "ACTION": "get", "id": input_set["id"]}
        with open(INPUT_GET_PATH, "w") as f:
            json.dump(input_get, f)

        subprocess.run(
            ["python", "singletonclient.py", f"-i={INPUT_GET_PATH}", f"-o={OUT_GET_PATH}", "-v"],
            cwd=BASE_DIR,
            check=True
        )
        assert os.path.exists(OUT_GET_PATH), "El archivo de salida GET no se generó"

        # --- Verificar log GET en DynamoDB ---
        row = wait_for_log(input_set["UUID"], "get", log_table)
        assert row is not None, "No se registró la acción GET en CorporateLog"
        print(f"[TEST] Log GET registrado correctamente")

        # --- ACCIÓN SUBSCRIBE ---
        print("\n[TEST] === PRUEBA SUBSCRIBE ===")
        input_sub = {"UUID": input_set["UUID"], "ACTION": "subscribe", "id": input_set["id"]}
        with open(INPUT_SUB_PATH, "w") as f:
            json.dump(input_sub, f)

        subprocess.run(
            ["python", "singletonclient.py", f"-i={INPUT_SUB_PATH}", "-v"],
            cwd=BASE_DIR,
            check=True
        )

        # --- Verificar log SUBSCRIBE en DynamoDB ---
        row = wait_for_log(input_set["UUID"], "subscribe", log_table)
        assert row is not None, "No se registró la acción SUBSCRIBE en CorporateLog"
        print(f"[TEST] Log SUBSCRIBE registrado correctamente")
        
        print("\n[TEST] TODOS LOS TESTS PASARON")

    finally:
        print("\n[TEST] Cerrando servidor...")
        server.terminate()
        
        # Dar tiempo para que el proceso termine
        try:
            server.wait(timeout=2)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        
        # Mostrar salida del servidor
        try:
            stdout, stderr = server.communicate(timeout=1)
            if stdout:
                print("\n[SERVIDOR STDOUT]:")
                print(stdout)
            if stderr:
                print("\n[SERVIDOR STDERR]:")
                print(stderr)
        except:
            pass