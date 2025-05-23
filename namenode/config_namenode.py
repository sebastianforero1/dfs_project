# config_namenode.py en el directorio namenode

import os

# --- Configuración de Red del NameNode ---

# La IP pública de la instancia EC2 donde corre el NameNode.
# El servidor escuchará en 'NAMENODE_LISTEN_HOST' (usualmente 0.0.0.0)
# pero esta IP es la que los clientes y DataNodes externos usarán para conectarse.
NAMENODE_PUBLIC_IP = os.environ.get("NAMENODE_PUBLIC_IP", "54.211.106.70") # Tu IP proporcionada

# Host en el que los servidores del NameNode escucharán.
# '0.0.0.0' permite escuchar en todas las interfaces de red disponibles.
NAMENODE_LISTEN_HOST = os.environ.get("NAMENODE_LISTEN_HOST", "0.0.0.0")

# Puerto para la API REST del NameNode (usada por los clientes).
NAMENODE_REST_PORT = int(os.environ.get("NAMENODE_REST_PORT", 8000))

# Puerto para los servicios gRPC del NameNode (usados por DataNodes y el NameNode seguidor).
NAMENODE_GRPC_PORT = int(os.environ.get("NAMENODE_GRPC_PORT", 50051))


# --- Configuración del Sistema de Archivos y Metadatos ---

# Tamaño de bloque por defecto para los archivos, en bytes.
# Ejemplo: 64MB = 64 * 1024 * 1024 bytes.
DEFAULT_BLOCK_SIZE_BYTES = int(os.environ.get("DEFAULT_BLOCK_SIZE_MB", 64)) * 1024 * 1024

# Número de réplicas que se deben mantener para cada bloque de archivo.
DEFAULT_REPLICATION_FACTOR = int(os.environ.get("DEFAULT_REPLICATION_FACTOR", 2)) # Mínimo 2 según el proyecto [cite: 23]

# Ruta y nombre del archivo donde se persistirán los metadatos del NameNode.
# Este archivo guardará un snapshot del estado del sistema de archivos.
METADATA_PERSISTENCE_FILE_PATH = os.environ.get("METADATA_PERSISTENCE_FILE", "namenode_fs_snapshot.json")


# --- Configuración de la Gestión de DataNodes ---

# Tiempo máximo (en segundos) que un DataNode puede estar sin enviar un heartbeat
# antes de ser considerado inactivo o "muerto" por el NameNode.
DATANODE_HEARTBEAT_TIMEOUT_SECONDS = int(os.environ.get("DATANODE_HEARTBEAT_TIMEOUT_SECONDS", 30))

# Intervalo (en segundos) con el que el NameNode ejecutará sus chequeos periódicos de salud
# (ej. verificar DataNodes inactivos, necesidades de replicación).
# Es buena idea que sea un poco más frecuente que el timeout, o una fracción de él.
NAMENODE_PERIODIC_CHECK_INTERVAL_SECONDS = int(os.environ.get("NAMENODE_PERIODIC_CHECK_INTERVAL_SECONDS", DATANODE_HEARTBEAT_TIMEOUT_SECONDS / 2))


# --- Agrupar configuraciones para pasar al MetadataManager ---
# Esto facilita pasar un solo objeto de configuración a la clase MetadataManager.
METADATA_MANAGER_CONFIG = {
    "BLOCK_SIZE": DEFAULT_BLOCK_SIZE_BYTES,
    "REPLICATION_FACTOR": DEFAULT_REPLICATION_FACTOR,
    "DATANODE_TIMEOUT_SECONDS": DATANODE_HEARTBEAT_TIMEOUT_SECONDS,
    "NAMENODE_METADATA_FILE": METADATA_PERSISTENCE_FILE_PATH
    # Se pueden añadir más configuraciones específicas del MetadataManager aquí si es necesario.
}

# --- Otras Configuraciones ---
# Aquí podrían ir configuraciones para logging, autenticación (si se implementa), etc.

# Ejemplo de configuración de logging (muy básico)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


# Para facilitar la importación, puedes imprimir un resumen de la configuración al cargar este módulo (opcional).
if __name__ == "__main__": # Esto solo se ejecuta si corres este archivo directamente
    print("--- Configuración del NameNode ---")
    print(f"IP Pública del NameNode: {NAMENODE_PUBLIC_IP}")
    print(f"Host de Escucha del NameNode: {NAMENODE_LISTEN_HOST}")
    print(f"Puerto REST API: {NAMENODE_REST_PORT}")
    print(f"Puerto gRPC: {NAMENODE_GRPC_PORT}")
    print(f"Tamaño de Bloque por Defecto: {DEFAULT_BLOCK_SIZE_BYTES // (1024*1024)} MB")
    print(f"Factor de Replicación por Defecto: {DEFAULT_REPLICATION_FACTOR}")
    print(f"Archivo de Persistencia de Metadatos: {METADATA_PERSISTENCE_FILE_PATH}")
    print(f"Timeout de Heartbeat de DataNode: {DATANODE_HEARTBEAT_TIMEOUT_SECONDS} s")
    print(f"Intervalo de Chequeo Periódico del NameNode: {NAMENODE_PERIODIC_CHECK_INTERVAL_SECONDS} s")
    print(f"Nivel de Logging: {LOG_LEVEL}")
    print("---------------------------------")