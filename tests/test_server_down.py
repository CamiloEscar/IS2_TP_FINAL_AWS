import subprocess
import json
import pytest
import socket

def is_server_running(host="localhost", port=8000):
    """Intenta conectar al servidor para verificar si está activo."""
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False

@pytest.mark.skipif(is_server_running(), reason="El servidor está activo; este test requiere que esté caído.")
def test_server_unavailable(tmp_path):
    """Verifica que el cliente maneje correctamente la falta de conexión al servidor."""

    # Crear archivo de entrada temporal
    input_data = {
        "UUID": "CPU-TEST-003",
        "ACTION": "list"
    }
    input_file = tmp_path / "input_down.json"
    output_file = tmp_path / "out_down.json"

    input_file.write_text(json.dumps(input_data))

    # Ejecutar el cliente (sin servidor disponible)
    result = subprocess.run(
        ["python", "src/singletonclient.py", f"-i={input_file}", f"-o={output_file}"],
        capture_output=True,
        text=True,
        check=False
    )

    # Debe retornar error
    assert result.returncode != 0, "El cliente debería devolver código de error si el servidor no está disponible."

    # Combinar stdout y stderr y verificar mensaje de error
    output = (result.stdout + result.stderr).lower()
    assert "error" in output or "connection" in output, (
        f"Se esperaba un mensaje de error de conexión, pero se obtuvo:\n{output}"
    )
