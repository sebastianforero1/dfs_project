# config_client.py en el directorio client

import os

# --- Configuración Principal del Cliente ---

# URL base de la API REST del NameNode.
# Esta es la dirección que el cliente usará para todas las operaciones de metadatos.
# Se intenta leer de una variable de entorno; si no existe, se usa un valor por defecto.
# Ejemplo de variable de entorno: export NAMENODE_REST_URL="http://54.211.106.70:8000"
DEFAULT_NAMENODE_REST_BASE_URL = "http://localhost:8000" # Valor por defecto para desarrollo local
NAMENODE_REST_URL = os.environ.get("NAMENODE_REST_URL", DEFAULT_NAMENODE_REST_BASE_URL)


# --- Timeouts para Operaciones de Red (en segundos) ---

# Timeout por defecto para las solicitudes HTTP (REST) al NameNode.
DEFAULT_HTTP_TIMEOUT_SECONDS = int(os.environ.get("CLIENT_HTTP_TIMEOUT_SECONDS", 15))

# Timeout por defecto para las operaciones gRPC con los DataNodes (ej. lectura/escritura de bloques).
# Estas operaciones pueden involucrar transferencia de datos y podrían necesitar más tiempo.
DEFAULT_GRPC_BLOCK_OP_TIMEOUT_SECONDS = int(os.environ.get("CLIENT_GRPC_BLOCK_OP_TIMEOUT_SECONDS", 60))


# --- Otras Configuraciones del Cliente (Ejemplos) ---

# Tamaño del búfer de lectura/escritura para operaciones de archivo local (si se implementa streaming granular).
# Actualmente, el SDK lee bloques completos en memoria.
# LOCAL_FILE_BUFFER_SIZE_BYTES = int(os.environ.get("CLIENT_LOCAL_FILE_BUFFER_SIZE_BYTES", 4 * 1024 * 1024)) # Ejemplo: 4MB

# Nivel de Logging para el cliente (si se implementa un logger más formal).
LOG_LEVEL = os.environ.get("CLIENT_LOG_LEVEL", "INFO").upper()


# Para facilitar la importación, puedes tener una función que devuelva un dict de configuración,
# o simplemente importar las variables directamente.
def get_client_config():
    return {
        "NAMENODE_REST_URL": NAMENODE_REST_URL,
        "HTTP_TIMEOUT_SECONDS": DEFAULT_HTTP_TIMEOUT_SECONDS,
        "GRPC_BLOCK_OP_TIMEOUT_SECONDS": DEFAULT_GRPC_BLOCK_OP_TIMEOUT_SECONDS,
        "LOG_LEVEL": LOG_LEVEL
        # "LOCAL_FILE_BUFFER_SIZE_BYTES": LOCAL_FILE_BUFFER_SIZE_BYTES,
    }

if __name__ == "__main__": # Para probar la carga de configuración
    config = get_client_config()
    print("--- Configuración del Cliente (desde config_client.py) ---")
    for key, value in config.items():
        print(f"{key}: {value}")
    
    # Ejemplo de cómo el SDK podría usar esto:
    # print(f"\nEl SDK se conectaría a NameNode en: {config['NAMENODE_REST_URL']}")
    print("------------------------------------------------------")