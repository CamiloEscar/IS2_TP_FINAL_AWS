# tests/test_invalid_args.py
import subprocess

def test_invalid_arguments():
    result = subprocess.run(["python", "src/singletonclient.py"], capture_output=True)
    assert result.returncode != 0
    stderr = result.stderr
    stdout = result.stdout
    # Buscamos "Error" o "usage" en ambos lugares por si argparse los manda a stderr
    assert b"Error" in stderr or b"usage" in stderr or b"Error" in stdout or b"usage" in stdout

