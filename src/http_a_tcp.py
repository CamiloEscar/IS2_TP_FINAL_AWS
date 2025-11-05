# proxy_http_tcp.py
from flask import Flask, request, jsonify
from flask_cors import CORS  # ← Necesitas instalar: pip install flask-cors
import socket
import json

app = Flask(__name__)
CORS(app) 

@app.route('/health', methods=['GET'])
def health():
    """Endpoint para verificar que el proxy está activo"""
    return jsonify({"status": "ok", "message": "Proxy HTTP activo"}), 200

@app.route('/api/execute', methods=['POST'])
def execute():
    data = request.json
    
    try:
        # Conectar al servidor TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', 8080))
            s.sendall(json.dumps(data).encode('utf-8'))
            
            # Recibir respuesta
            response_data = b''
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response_data += chunk
                if len(chunk) < 4096:
                    break
            
            response = response_data.decode('utf-8')
            return jsonify(json.loads(response))
    
    except Exception as e:
        return jsonify({"Error": str(e)}), 500

if __name__ == '__main__':
    print(" Proxy HTTP iniciado en http://localhost:5000")
    print(" Conectando al servidor TCP en localhost:8080")
    app.run(port=5000, debug=True)