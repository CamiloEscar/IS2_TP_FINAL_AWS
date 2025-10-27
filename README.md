# repositorio TP FINAL IS2

## Sistema Singleton-Proxy-Observer con AWS DynamoDB

Sistema de gestión de datos corporativos implementando patrones de diseño Singleton, Proxy y Observer con AWS DynamoDB.

## Componentes

### 1. singletonproxyobserver.py

Servidor principal que maneja las conexiones y operaciones con DynamoDB.

**Características:**

- Patrón Singleton para acceso a tablas DynamoDB
- Patrón Proxy para intermediar solicitudes
- Patrón Observer para notificaciones en tiempo real
- Thread-safe con locks para operaciones concurrentes
- Validación robusta de datos antes de insertar en DynamoDB
- Conversión automática de tipos para compatibilidad con DynamoDB

### 2. singletonclient.py

Cliente para enviar operaciones (get/set/list) al servidor.

**Uso:**
\`\`\`bash

#### Insertar datos

python singletonclient.py -i input.json -v

#### Con servidor personalizado

python singletonclient.py -i input.json -s localhost -p 8080 -v

#### Guardar respuesta en archivo

python singletonclient.py -i input.json -o output.json
\`\`\`

### 3. observerclient.py

Cliente que se suscribe para recibir actualizaciones en tiempo real.

**Uso:**
\`\`\`bash

#### Suscribirse al servidor

python observerclient.py -v

#### Guardar actualizaciones en archivo

python observerclient.py -o updates.log -v
\`\`\`

### 4. test_TPFI.py

Script de diagnóstico para verificar la conexión con DynamoDB.

**Uso:**
\`\`\`bash
python test_TPFI.py
\`\`\`

## Formato de Datos

### Archivo input.json

\`\`\`json
{
  "ACTION": "set",
  "id": "UADER-FCyT-IS2",
  "cp": "3260",
  "CUIT": "30-70925411-8",
  "domicilio": "25 de Mayo 385-1P",
  "localidad": "Concepción del Uruguay",
  "provincia": "Entre Rios",
  "sede": "FCyT",
  "telefono": "03442 43-1442",
  "web": "<http://www.uader.edu.ar>"
}
\`\`\`

**Nota:** El campo `id` es obligatorio para operaciones SET.

## Acciones Disponibles

- **set**: Insertar o actualizar un registro (requiere campo `id`)
- **get**: Obtener un registro por ID
- **list**: Listar todos los registros
- **subscribe**: Suscribirse para recibir actualizaciones en tiempo real

### Verificar estructura de la tabla

\`\`\`bash
aws dynamodb describe-table --table-name CorporateData --region us-east-1
\`\`\`

## Requisitos

- Python 3.7+
- boto3
- Credenciales AWS configuradas en `~/.aws/credentials`
- Tabla `CorporateData` en DynamoDB con clave primaria `id` (String)
- Tabla `CorporateLog` en DynamoDB para auditoría
