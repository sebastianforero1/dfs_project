# config_datanode.py en el directorio datanode (Opcional)

import os

# --- Configuración de Identificación y Red del DataNode ---

# ID único para esta instancia de DataNode.
# Se recomienda encarecidamente establecer esto mediante una variable de entorno.
DATANODE_ID = os.environ.get("DATANODE_ID", f"dn_default_config_{os.getpid()}")

# Host en el que el servidor gRPC del DataNode escuchará.
DATANODE_GRPC_LISTEN_HOST = os.environ.get("DATANODE_GRPC_LISTEN_HOST", "0.0.0.0")

# Puerto en el que el servidor gRPC del DataNode escuchará.
# DEBE SER ÚNICO POR INSTANCIA DE DATANODE si corren en la misma máquina.
DATANODE_GRPC_PORT = int(os.environ.get("DATANODE_GRPC_PORT", 50061))

# La dirección (IP:puerto) que este DataNode anunciará al NameNode.
# ES CRUCIAL CONFIGURAR ESTO CORRECTAMENTE CON LA IP PÚBLICA EN AWS.
ANNOUNCED_DATANODE_GRPC_ADDRESS = os.environ.get(
    "ANNOUNCED_DATANODE_GRPC_ADDRESS",
    f"localhost:{DATANODE_GRPC_PORT}" # Valor por defecto muy básico.
)

# --- Configuración de Almacenamiento ---

# Directorio base donde este DataNode almacenará sus bloques.
# Se creará un subdirectorio con DATANODE_ID dentro de esta ruta.
DATANODE_STORAGE_BASE_DIR = os.environ.get("DATANODE_STORAGE_BASE_DIR", "./datanode_data_storage_via_config")

# --- Configuración de Comunicación con el NameNode ---

# Dirección gRPC (IP:Puerto) del NameNode.
NAMENODE_GRPC_TARGET_ADDRESS = os.environ.get("NAMENODE_GRPC_TARGET_ADDRESS", "localhost:50051")

# Intervalo en segundos para enviar heartbeats al NameNode.
HEARTBEAT_INTERVAL_SECONDS = int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", 15))

# --- Otras Configuraciones (Ejemplos) ---
# Máximo de workers para el ThreadPoolExecutor del servidor gRPC del DataNode.
GRPC_SERVER_MAX_WORKERS = int(os.environ.get("DATANODE_GRPC_MAX_WORKERS", 20))

# Nivel de Logging para el DataNode (ej. INFO, DEBUG)
LOG_LEVEL = os.environ.get("DATANODE_LOG_LEVEL", "INFO").upper()


# Podrías tener una función para cargar y validar la configuración si fuera necesario.
def get_datanode_configurations():
    # Aquí se podría añadir lógica de validación si se desea.
    # Por ejemplo, verificar que los puertos sean números válidos, etc.
    return {
        "DATANODE_ID": DATANODE_ID,
        "DATANODE_GRPC_LISTEN_HOST": DATANODE_GRPC_LISTEN_HOST,
        "DATANODE_GRPC_PORT": DATANODE_GRPC_PORT,
        "ANNOUNCED_DATANODE_GRPC_ADDRESS": ANNOUNCED_DATANODE_GRPC_ADDRESS,
        "DATANODE_STORAGE_BASE_DIR": DATANODE_STORAGE_BASE_DIR,
        "NAMENODE_GRPC_TARGET_ADDRESS": NAMENODE_GRPC_TARGET_ADDRESS,
        "HEARTBEAT_INTERVAL_SECONDS": HEARTBEAT_INTERVAL_SECONDS,
        "GRPC_SERVER_MAX_WORKERS": GRPC_SERVER_MAX_WORKERS,
        "LOG_LEVEL": LOG_LEVEL
    }

if __name__ == "__main__": # Para probar la carga de configuración
    config = get_datanode_configurations()
    print("--- Configuración del DataNode (desde config_datanode.py) ---")
    for key, value in config.items():
        print(f"{key}: {value}")
    print("---------------------------------------------------------")