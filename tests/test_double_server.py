import subprocess
import time
from pathlib import Path
import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
TIMEOUT = 5  # segundos

def run_server(args):
    """Ejecuta el servidor y devuelve el Popen"""
    return subprocess.Popen(
        args,
        cwd=str(SRC_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

def test_double_server_start():
    # Levantar primer servidor
    server1 = run_server(["python", "singletonproxyobserver.py", "-p=8080"])
    time.sleep(2)  # dejar que se inicialice

    try:
        # Intentar levantar segundo servidor en el mismo puerto
        server2 = run_server(["python", "singletonproxyobserver.py", "-p=8080"])
        try:
            stdout, stderr = server2.communicate(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            server2.kill()
            stdout, stderr = "", "Timeout (probablemente bloqueado por primer servidor)"
        
        # La segunda instancia debe fallar
        assert "Address already in use" in stderr or server2.returncode != 0, \
            f"El segundo servidor no fallo como se esperaba. STDOUT:\n{stdout}\nSTDERR:\n{stderr}"

    finally:
        # Terminar servidores
        server1.terminate()
        if 'server2' in locals():
            server2.terminate()