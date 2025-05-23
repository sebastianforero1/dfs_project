# main_datanode.py en el directorio datanode (UTILIZANDO config_datanode.py)

import asyncio
import grpc
from concurrent import futures
# import os # Ya no es tan necesario aquí para leer env vars directamente, config_datanode lo hace

# Importar el módulo de configuración del DataNode
from . import config_datanode as cfg # Usamos 'cfg' como alias

# Importaciones de módulos locales del paquete datanode
from .grpc_services import DataNodeOperationsServicer
from .block_store import BlockStore
from .heartbeat_client import run_heartbeat_sender

# Importaciones de los stubs generados por gRPC
import dfs_pb2_grpc

# --- Cargar Configuraciones desde config_datanode.py ---
# Accedemos a las variables directamente desde el módulo 'cfg'
DATANODE_ID = cfg.DATANODE_ID
DATANODE_GRPC_LISTEN_HOST = cfg.DATANODE_GRPC_LISTEN_HOST
DATANODE_GRPC_PORT = cfg.DATANODE_GRPC_PORT
ANNOUNCED_DATANODE_GRPC_ADDRESS = cfg.ANNOUNCED_DATANODE_GRPC_ADDRESS
DATANODE_STORAGE_BASE_DIR = cfg.DATANODE_STORAGE_BASE_DIR
NAMENODE_GRPC_TARGET_ADDRESS = cfg.NAMENODE_GRPC_TARGET_ADDRESS
HEARTBEAT_INTERVAL_SECONDS = cfg.HEARTBEAT_INTERVAL_SECONDS
GRPC_SERVER_MAX_WORKERS = cfg.GRPC_SERVER_MAX_WORKERS # Nueva config desde config_datanode
# LOG_LEVEL = cfg.LOG_LEVEL # Podría usarse para configurar un logger más avanzado si se implementa

# --- Instancias Globales (específicas para esta instancia de DataNode) ---

block_store_instance = BlockStore(
    data_dir_base_path=DATANODE_STORAGE_BASE_DIR,
    datanode_id=DATANODE_ID
)

# --- Inyección de Dependencias (o configuración de módulos) ---

from . import grpc_services as datanode_grpc_module
datanode_grpc_module.block_store_instance = block_store_instance
datanode_grpc_module.THIS_DATANODE_ID = DATANODE_ID
datanode_grpc_module.THIS_DATANODE_GRPC_ADDRESS = ANNOUNCED_DATANODE_GRPC_ADDRESS

from . import heartbeat_client as datanode_heartbeat_module
datanode_heartbeat_module.block_store_instance = block_store_instance
datanode_heartbeat_module.THIS_DATANODE_ID = DATANODE_ID
datanode_heartbeat_module.THIS_DATANODE_GRPC_ADDRESS = ANNOUNCED_DATANODE_GRPC_ADDRESS
datanode_heartbeat_module.NAMENODE_GRPC_TARGET_ADDRESS = NAMENODE_GRPC_TARGET_ADDRESS
datanode_heartbeat_module.HEARTBEAT_INTERVAL_SECONDS = HEARTBEAT_INTERVAL_SECONDS


async def serve_datanode_grpc_server():
    """Configura e inicia el servidor gRPC principal del DataNode."""
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=GRPC_SERVER_MAX_WORKERS))
    
    dfs_pb2_grpc.add_DataNodeOperationsServicer_to_server(
        DataNodeOperationsServicer(),
        server
    )
    
    listen_addr = f"{DATANODE_GRPC_LISTEN_HOST}:{DATANODE_GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    
    print(f"DataNode [{DATANODE_ID}]: Servidor gRPC escuchando en {listen_addr}.")
    print(f"DataNode [{DATANODE_ID}]: Anunciándose al NameNode como {ANNOUNCED_DATANODE_GRPC_ADDRESS}.")
    print(f"DataNode [{DATANODE_ID}]: Almacenamiento de bloques en: {block_store_instance.data_dir_for_this_dn}")
    
    await server.start()
    
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"DataNode [{DATANODE_ID}]: Servidor gRPC detenido por el usuario.")
    finally:
        print(f"DataNode [{DATANODE_ID}]: Deteniendo servidor gRPC...")
        await server.stop(0)
        print(f"DataNode [{DATANODE_ID}]: Servidor gRPC detenido.")


async def main_datanode_instance():
    """Función principal para iniciar todos los servicios de esta instancia de DataNode."""
    print(f"DataNode [{DATANODE_ID}]: Iniciando servicios con configuración de 'config_datanode.py'...")

    # Crear el directorio de datos (BlockStore.__init__ ya se encarga de esto)
    # block_store_instance.ensure_data_directory_exists()

    # Cargar/Reconstruir la lista de bloques existentes (BlockStore.__init__ ya se encarga de esto)
    # block_store_instance.initialize_and_load_block_index()

    heartbeat_task = asyncio.create_task(run_heartbeat_sender())
    grpc_server_task = asyncio.create_task(serve_datanode_grpc_server())
    
    try:
        await asyncio.gather(heartbeat_task, grpc_server_task)
    except Exception as e:
        print(f"DataNode [{DATANODE_ID}] ERROR: Excepción en una tarea principal: {e}")
    finally:
        print(f"DataNode [{DATANODE_ID}]: Todos los servicios han concluido su ejecución.")
        if not heartbeat_task.done():
            heartbeat_task.cancel()
        if not grpc_server_task.done():
            grpc_server_task.cancel()
        await asyncio.sleep(0.1) # Dar tiempo para que las cancelaciones se procesen


def run_datanode_instance():
    """Punto de entrada para ejecutar esta instancia de DataNode."""
    # Pequeña validación/recordatorio para configuración en AWS
    # (La variable os.environ ya no se usa directamente aquí, pero cfg.ANNOUNCED_DATANODE_GRPC_ADDRESS
    # y cfg.NAMENODE_GRPC_TARGET_ADDRESS la habrán leído. La heurística puede seguir siendo útil.)
    # Esta heurística es muy simple y podría necesitar ajustes o basarse en otras detecciones de entorno.
    is_likely_aws_environment = "AWS_EXECUTION_ENV" in os.environ or "EC2_INSTANCE_ID" in os.environ # Ejemplo de detección
    
    if "localhost" in ANNOUNCED_DATANODE_GRPC_ADDRESS and is_likely_aws_environment:
        print(f"DataNode [{DATANODE_ID}] ADVERTENCIA: ANNOUNCED_DATANODE_GRPC_ADDRESS ('{ANNOUNCED_DATANODE_GRPC_ADDRESS}') parece ser local, pero podría estar en un entorno de nube. Asegúrate de que sea la IP pública/alcanzable correcta.")
    if "localhost" in NAMENODE_GRPC_TARGET_ADDRESS and is_likely_aws_environment:
        print(f"DataNode [{DATANODE_ID}] ADVERTENCIA: NAMENODE_GRPC_TARGET_ADDRESS ('{NAMENODE_GRPC_TARGET_ADDRESS}') parece ser local. En un entorno de nube, usa la IP privada (o pública si está en otra VPC) del NameNode.")

    try:
        asyncio.run(main_datanode_instance())
    except KeyboardInterrupt:
        print(f"DataNode [{DATANODE_ID}]: Proceso principal detenido por el usuario (KeyboardInterrupt).")
    finally:
        print(f"DataNode [{DATANODE_ID}]: Servicio DataNode finalizado.")


if __name__ == "__main__":
    print(f"DataNode: Lanzando instancia de DataNode (ID predeterminado por config: {cfg.DATANODE_ID}). Configuración leída de 'config_datanode.py'.")
    print("Asegúrate de que las variables de entorno estén configuradas si deseas anular los valores por defecto de 'config_datanode.py'.")
    run_datanode_instance()