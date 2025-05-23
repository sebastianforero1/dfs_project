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
from . import api_endpoints as api_module
from . import grpc_services as grpc_module
api_module.metadata_manager = metadata_manager_instance
grpc_module.metadata_manager = metadata_manager_instance


async def serve_rest_api():
    """Configura y ejecuta el servidor FastAPI para la API REST."""
    config = uvicorn.Config(
        app_namenode_rest,
        host=cfg.NAMENODE_LISTEN_HOST,
        port=cfg.NAMENODE_REST_PORT,
        log_level=cfg.LOG_LEVEL.lower()
    )
    server = uvicorn.Server(config)
    print(f"NameNode (DEBUG): API REST lista para iniciar en http://{cfg.NAMENODE_LISTEN_HOST}:{cfg.NAMENODE_REST_PORT}")
    await server.serve()
    print(f"NameNode (DEBUG): API REST ha terminado.") # Solo se verá si Uvicorn se detiene


async def serve_grpc_services():
    """Configura y ejecuta el servidor gRPC para los DataNodes y el Seguidor."""
    print(f"NameNode (DEBUG): Entrando a serve_grpc_services...")
    server = None # Inicializar server a None
    health_check_task = None # Inicializar health_check_task a None
    try:
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        
        dfs_pb2_grpc.add_NameNodeManagementServicer_to_server(
            NameNodeManagementServicer(),
            server
        )
        
        listen_addr = f"{cfg.NAMENODE_LISTEN_HOST}:{cfg.NAMENODE_GRPC_PORT}"
        server.add_insecure_port(listen_addr)
        print(f"NameNode (DEBUG): Puerto gRPC añadido: {listen_addr}")
        
        await server.start()
        print(f"NameNode: Servidor gRPC escuchando en {listen_addr} (accesible vía {cfg.NAMENODE_PUBLIC_IP}:{cfg.NAMENODE_GRPC_PORT} por DataNodes)")
        
        async def periodic_system_health_checks():
            print(f"NameNode (DEBUG): Iniciando tarea de chequeos periódicos de salud del sistema cada {cfg.NAMENODE_PERIODIC_CHECK_INTERVAL_SECONDS} segundos.")
            try:
                while True:
                    await asyncio.sleep(cfg.NAMENODE_PERIODIC_CHECK_INTERVAL_SECONDS)
                    print(f"NameNode (DEBUG): Ejecutando chequeo periódico...")
                    if metadata_manager_instance: # Asegurarse que la instancia existe
                        metadata_manager_instance.perform_periodic_checks()
                    else:
                        print("NameNode (DEBUG) ERROR: metadata_manager_instance no está disponible para chequeos.")
            except asyncio.CancelledError:
                print("NameNode (DEBUG): Tarea de chequeos de salud cancelada.")
                raise 
            except Exception as e_health:
                print(f"NameNode (DEBUG) ERROR en health_checks: {e_health}")
                import traceback
                traceback.print_exc()


        health_check_task = asyncio.create_task(periodic_system_health_checks())
        print(f"NameNode (DEBUG): Tarea de chequeos de salud creada.")
        
        await server.wait_for_termination()

    except Exception as e_grpc_server:
        print(f"NameNode (DEBUG) ERROR CRÍTICO en serve_grpc_services: {e_grpc_server}")
        import traceback
        traceback.print_exc()
        raise # Re-lanzar para que el gather lo vea si es posible
    finally:
        print(f"NameNode (DEBUG): Saliendo de serve_grpc_services (finally block)...")
        if health_check_task and not health_check_task.done():
            print(f"NameNode (DEBUG): Cancelando tarea de chequeos (en finally)...")
            health_check_task.cancel()
            try:
                await health_check_task
            except asyncio.CancelledError:
                print("NameNode (DEBUG): Tarea de chequeos (en finally) cancelada exitosamente.")
            except Exception as e_task_cancel:
                print(f"NameNode (DEBUG) ERROR al esperar cancelación de health_check_task: {e_task_cancel}")
        
        if server: 
             print(f"NameNode (DEBUG): Deteniendo servidor gRPC (en finally)...")
             # El timeout de stop debería ser pequeño o None para esperar indefinidamente si es necesario.
             # 0 es para una detención rápida, pero puede dejar cosas en el aire.
             # Para un apagado más grácil, podrías usar un timeout mayor o None.
             await server.stop(grace=1.0) # Añadir un pequeño tiempo de gracia
             print(f"NameNode (DEBUG): Servidor gRPC detenido (en finally).")


async def main_namenode_server():
    """Función principal para iniciar todos los servicios del NameNode."""
    print(f"NameNode (DEBUG): Iniciando main_namenode_server. IP pública: {cfg.NAMENODE_PUBLIC_IP}.")
    try:
        # --- Configuración Final: Ejecutar ambos servicios concurrentemente ---
        print("NameNode (DEBUG): INTENTANDO INICIAR AMBOS SERVICIOS CON ASYNCIO.GATHER()...")
        await asyncio.gather(
            serve_rest_api(),    # Corutina para el servidor REST (FastAPI/Uvicorn)
            serve_grpc_services()  # Corutina para el servidor gRPC y chequeos periódicos
        )
        # Esta línea solo se alcanzaría si AMBAS tareas en gather terminan normalmente (lo cual no suele pasar con servidores).
        print("NameNode (DEBUG): main_namenode_server (asyncio.gather) ha concluido (esto es inusual para servidores).")

    except Exception as e_main:
        print(f"NameNode (DEBUG) ERROR CRÍTICO en main_namenode_server (asyncio.gather): {e_main}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"NameNode (DEBUG): main_namenode_server (bloque finally) ha concluido.")


def run_namenode_service():
    """Punto de entrada para ejecutar el NameNode."""
    try:
        asyncio.run(main_namenode_server())
    except KeyboardInterrupt:
        print("NameNode: Proceso principal del NameNode detenido por el usuario (KeyboardInterrupt).")
    except SystemExit as e: # Capturar SystemExit que Uvicorn podría lanzar
        print(f"NameNode: SystemExit capturado en run_namenode_service con código: {e.code}")
    except Exception as e_run: # Capturar cualquier otra excepción no esperada
        print(f"NameNode ERROR INESPERADO en run_namenode_service: {e_run}")
        import traceback
        traceback.print_exc()
    finally:
        print("NameNode: Servicio NameNode finalizado.")


if __name__ == "__main__":
    print("NameNode: Lanzando el servicio NameNode desde __main__...")
    run_namenode_service()