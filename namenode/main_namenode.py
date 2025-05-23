# main_namenode.py en el directorio namenode

import asyncio
import grpc
from concurrent import futures
import uvicorn
# import os # Ya no es tan necesario aquí para leer env vars directamente

# Importar el módulo de configuración
from . import config_namenode as cfg # Usamos 'cfg' como alias

# Importaciones de módulos locales del paquete namenode
from .api_endpoints import app_namenode_rest # Aplicación FastAPI
from .grpc_services import NameNodeManagementServicer # Implementación del servicio gRPC del NameNode
from .metadata_manager import MetadataManager # Clase para gestionar los metadatos

# Importaciones de los stubs generados por gRPC
import dfs_pb2_grpc # Asegúrate que este archivo esté en la raíz del proyecto o PYTHONPATH

# --- Instancia Global del MetadataManager ---
# Se inicializa con la configuración agrupada desde config_namenode.py
metadata_manager_instance = MetadataManager(config=cfg.METADATA_MANAGER_CONFIG)

# --- Inyección de Dependencias ---
# Asignar la instancia de metadata_manager a los módulos que la necesitan.
# Esto se hace para que los endpoints de API y los servicios gRPC puedan acceder
# a la lógica de negocio y al estado gestionado por el MetadataManager.
from . import api_endpoints as api_module
from . import grpc_services as grpc_module
api_module.metadata_manager = metadata_manager_instance
grpc_module.metadata_manager = metadata_manager_instance


async def serve_rest_api():
    """Configura y ejecuta el servidor FastAPI para la API REST."""
    # La configuración de Uvicorn ahora usa los valores del módulo 'cfg'
    config = uvicorn.Config(
        app_namenode_rest,
        host=cfg.NAMENODE_LISTEN_HOST,
        port=cfg.NAMENODE_REST_PORT,
        log_level=cfg.LOG_LEVEL.lower() # Asegurarse que el log_level esté en minúsculas para uvicorn
    )
    server = uvicorn.Server(config)
    print(f"NameNode: API REST escuchando en http://{cfg.NAMENODE_LISTEN_HOST}:{cfg.NAMENODE_REST_PORT} (accesible vía {cfg.NAMENODE_PUBLIC_IP}:{cfg.NAMENODE_REST_PORT})")
    await server.serve()

async def serve_grpc_services():
    """Configura y ejecuta el servidor gRPC para los DataNodes y el Seguidor."""
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Añadir el servicer para las operaciones de gestión de DataNodes
    dfs_pb2_grpc.add_NameNodeManagementServicer_to_server(
        NameNodeManagementServicer(), # Esta instancia ya tiene metadata_manager_instance inyectado indirectamente
        server
    )
    
    listen_addr = f"{cfg.NAMENODE_LISTEN_HOST}:{cfg.NAMENODE_GRPC_PORT}"
    server.add_insecure_port(listen_addr) # El NameNode escuchará en el host y puerto definidos en la config
    print(f"NameNode: Servidor gRPC escuchando en {listen_addr} (accesible vía {cfg.NAMENODE_PUBLIC_IP}:{cfg.NAMENODE_GRPC_PORT} por DataNodes)")
    
    await server.start()
    
    # Tarea periódica para la salud del sistema
    async def periodic_system_health_checks():
        # La carga inicial de metadatos ya la maneja el constructor de MetadataManager
        print(f"NameNode: Iniciando tarea de chequeos periódicos de salud del sistema cada {cfg.NAMENODE_PERIODIC_CHECK_INTERVAL_SECONDS} segundos.")
        while True:
            await asyncio.sleep(cfg.NAMENODE_PERIODIC_CHECK_INTERVAL_SECONDS)
            # print(f"NameNode: Ejecutando chequeos periódicos de salud del sistema...") # Puede ser muy verboso
            try:
                metadata_manager_instance.perform_periodic_checks()
            except Exception as e:
                print(f"NameNode ERROR: Excepción durante chequeos periódicos: {e}")
            # El guardado de metadatos ahora se maneja dentro de perform_periodic_checks o en _save_metadata_to_file si hay cambios.

    # Iniciar la tarea de chequeos periódicos en segundo plano
    health_check_task = asyncio.create_task(periodic_system_health_checks())
    
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        print("NameNode: Servidor gRPC detenido por el usuario.")
    finally:
        print("NameNode: Deteniendo tarea de chequeos de salud...")
        health_check_task.cancel() # Cancelar la tarea de chequeos
        try:
            await health_check_task # Esperar a que la cancelación se complete
        except asyncio.CancelledError:
            print("NameNode: Tarea de chequeos de salud cancelada exitosamente.")
        
        print("NameNode: Deteniendo servidor gRPC...")
        await server.stop(0) # 0 es el tiempo de gracia, detener inmediatamente
        print("NameNode: Servidor gRPC detenido.")
        # Asegurar guardado final de metadatos. MetadataManager podría tener un método close() o __del__ para esto.
        # O simplemente llamar a save si es necesario aquí, aunque debería hacerse dentro de los métodos que modifican.
        # metadata_manager_instance._save_metadata_to_file() # Considerar si es necesario un guardado final explícito aquí.


async def main_namenode_server():
    """Función principal para iniciar todos los servicios del NameNode."""
    print(f"NameNode: Iniciando servicios. IP pública configurada: {cfg.NAMENODE_PUBLIC_IP}.")
    
    # La carga inicial de metadatos es manejada por el constructor de MetadataManager.

    try:
        # Iniciar ambos servidores concurrentemente
        await asyncio.gather(
            serve_rest_api(),
            serve_grpc_services()
        )
    except Exception as e:
        print(f"NameNode ERROR: Excepción en main_namenode_server: {e}")
    finally:
        print("NameNode: Todos los servicios del NameNode han concluido su ejecución.")


def run_namenode_service():
    """Punto de entrada para ejecutar el NameNode."""
    try:
        asyncio.run(main_namenode_server())
    except KeyboardInterrupt:
        print("NameNode: Proceso principal del NameNode detenido por el usuario (KeyboardInterrupt).")
    finally:
        print("NameNode: Servicio NameNode finalizado.")
        # No es ideal llamar a _save_metadata_to_file() aquí explícitamente,
        # ya que el objeto metadata_manager_instance podría ya no estar en un estado predecible
        # si la detención fue abrupta. El guardado debe ser gestionado por los métodos de MetadataManager
        # o en los bloques `finally` de los servidores que lo usan.

if __name__ == "__main__":
    # Este bloque permite ejecutar el NameNode directamente con:
    # python -m namenode.main_namenode
    # desde el directorio raíz del proyecto (donde está la carpeta 'namenode').
    print("NameNode: Lanzando el servicio NameNode desde __main__...")
    run_namenode_service()