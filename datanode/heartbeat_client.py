# heartbeat_client.py en el directorio datanode

import asyncio
import grpc
import time # Para el intervalo del heartbeat

# Importaciones de los stubs generados por gRPC
# Asegúrate de que dfs_service_pb2.py y dfs_service_pb2_grpc.py
# estén en la raíz del proyecto o en una ubicación accesible por PYTHONPATH.
import dfs_pb2
import dfs_pb2_grpc

# --- Variables de Configuración (serán establecidas por main_datanode.py) ---
# Estas variables se configuran mediante "inyección" desde main_datanode.py
# para que este módulo no dependa directamente de os.environ.

block_store_instance = None # Instancia de BlockStore para obtener la lista de bloques y espacio
THIS_DATANODE_ID = "datanode_unconfigured_hb"
THIS_DATANODE_GRPC_ADDRESS = "localhost:0" # Dirección que este DataNode anuncia
NAMENODE_GRPC_TARGET_ADDRESS = "localhost:50051" # Dirección del NameNode
HEARTBEAT_INTERVAL_SECONDS = 15


async def run_heartbeat_sender():
    """
    Tarea asíncrona que envía heartbeats periódicamente al NameNode.
    Esta función se ejecuta en un bucle continuo hasta que se cancela.
    """
    print(f"DataNode [{THIS_DATANODE_ID}]: Iniciando cliente de heartbeats hacia NameNode en {NAMENODE_GRPC_TARGET_ADDRESS}.")
    
    while True:
        try:
            # Crear un nuevo canal para cada intento de heartbeat podría ser menos eficiente
            # que mantener un canal abierto, pero es más robusto ante desconexiones temporales del NameNode.
            # Para un sistema de producción, se podría considerar una estrategia de reconexión más sofisticada.
            async with grpc.aio.insecure_channel(NAMENODE_GRPC_TARGET_ADDRESS) as channel:
                stub = dfs_pb2_grpc.NameNodeManagementStub(channel)

                # 1. Obtener la lista de IDs de bloques almacenados actualmente por este DataNode.
                #    Esto DEBE ser implementado correctamente en BlockStore.
                #    BlockStore debería tener un método que devuelva una lista/set de los block_ids que gestiona.
                if block_store_instance is None:
                    print(f"DataNode [{THIS_DATANODE_ID}] ERROR HB: BlockStore no inicializado. No se puede enviar el reporte de bloques.")
                    stored_block_ids_list = []
                else:
                    # Asumimos que BlockStore tiene un método que devuelve los IDs de los bloques.
                    # Este método debería escanear el directorio o mantener una lista en memoria.
                    stored_block_ids_list = block_store_instance.get_stored_block_ids()
                    # print(f"DataNode [{THIS_DATANODE_ID}] DEBUG HB: Bloques reportados: {stored_block_ids_list}")


                # 2. Obtener el espacio de almacenamiento disponible.
                if block_store_instance is None:
                    available_storage = 0
                else:
                    available_storage = block_store_instance.get_available_storage_bytes()

                # 3. Preparar la solicitud de heartbeat.
                heartbeat_request = dfs_pb2.HeartbeatRequest(
                    datanode_info=dfs_pb2.DataNodeLocation(
                        datanode_id=THIS_DATANODE_ID,
                        datanode_address=THIS_DATANODE_GRPC_ADDRESS # La dirección que este DN anuncia
                    ),
                    block_ids_stored=stored_block_ids_list,
                    available_storage_bytes=available_storage
                )

                # print(f"DataNode [{THIS_DATANODE_ID}]: Enviando heartbeat al NameNode. Bloques: {len(stored_block_ids_list)}, Espacio libre: {available_storage // (1024*1024)}MB.")
                
                # 4. Enviar el heartbeat y obtener la respuesta.
                # Se podría añadir un timeout a esta llamada gRPC.
                response = await stub.SendHeartbeat(heartbeat_request, timeout=(HEARTBEAT_INTERVAL_SECONDS / 2))

                if response and response.commands_from_namenode:
                    print(f"DataNode [{THIS_DATANODE_ID}]: Comandos recibidos del NameNode en respuesta al heartbeat: {list(response.commands_from_namenode)}")
                    # Procesar los comandos recibidos del NameNode.
                    # Esta lógica de procesamiento de comandos debería estar aquí o en otro módulo.
                    await process_namenode_commands(response.commands_from_namenode)

        except grpc.aio.AioRpcError as e:
            # Errores comunes:
            # - grpc.StatusCode.UNAVAILABLE: NameNode no está accesible.
            # - grpc.StatusCode.DEADLINE_EXCEEDED: Timeout de la llamada.
            print(f"DataNode [{THIS_DATANODE_ID}] ADVERTENCIA HB: No se pudo enviar el heartbeat al NameNode ({NAMENODE_GRPC_TARGET_ADDRESS}). Error gRPC: {e.code()} - {e.details()}")
        except Exception as e_generic:
            # Capturar otras excepciones inesperadas para evitar que la tarea de heartbeat muera.
            print(f"DataNode [{THIS_DATANODE_ID}] ERROR HB: Excepción inesperada en el bucle de heartbeats: {e_generic}")
        
        # Esperar el intervalo definido antes de enviar el siguiente heartbeat.
        # Esta espera se realiza incluso si hubo un error, para no sobrecargar la red o el NameNode con reintentos rápidos.
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)


async def process_namenode_commands(commands: list):
    """
    Procesa los comandos recibidos del NameNode en la respuesta del heartbeat.
    Ejemplos de comandos: "DELETE_BLOCK:block_id_X", "REPLICATE_BLOCK:block_id_Y:TARGET_DN_ADDR:target_ip:port"
    """
    if block_store_instance is None:
        print(f"DataNode [{THIS_DATANODE_ID}] ERROR CMD: BlockStore no inicializado. No se pueden procesar comandos del NameNode.")
        return

    for command_str in commands:
        print(f"DataNode [{THIS_DATANODE_ID}]: Procesando comando: '{command_str}'")
        try:
            parts = command_str.split(':')
            command_type = parts[0]

            if command_type == "DELETE_BLOCK":
                if len(parts) >= 2:
                    block_id_to_delete = parts[1]
                    print(f"DataNode [{THIS_DATANODE_ID}]: Ejecutando comando DELETE_BLOCK para el bloque: {block_id_to_delete}")
                    block_store_instance.delete_block(block_id_to_delete)
                else:
                    print(f"DataNode [{THIS_DATANODE_ID}] ERROR CMD: Comando DELETE_BLOCK malformado: {command_str}")
            
            elif command_type == "REPLICATE_BLOCK":
                # Formato esperado: REPLICATE_BLOCK:block_id_to_replicate:TARGET_DN_ADDR:target_dn_ip_puerto
                if len(parts) >= 4 and parts[2] == "TARGET_DN_ADDR":
                    block_id_to_replicate = parts[1]
                    target_datanode_address = parts[3] # Ej: "54.221.53.163:50062"
                    print(f"DataNode [{THIS_DATANODE_ID}]: Ejecutando comando REPLICATE_BLOCK para el bloque {block_id_to_replicate} hacia {target_datanode_address}")
                    
                    # Esta es una operación que puede llevar tiempo y es I/O bound (red y disco).
                    # Debería ejecutarse de forma asíncrona para no bloquear el procesamiento de otros comandos o heartbeats.
                    # Aquí se llamaría a una función que maneje la lógica de replicación.
                    asyncio.create_task(
                        handle_block_replication_command(block_id_to_replicate, target_datanode_address)
                    )
                else:
                    print(f"DataNode [{THIS_DATANODE_ID}] ERROR CMD: Comando REPLICATE_BLOCK malformado: {command_str}")
            
            else:
                print(f"DataNode [{THIS_DATANODE_ID}] ADVERTENCIA CMD: Tipo de comando desconocido recibido del NameNode: {command_type}")

        except Exception as e:
            print(f"DataNode [{THIS_DATANODE_ID}] ERROR CMD: Excepción al procesar el comando '{command_str}': {e}")


async def handle_block_replication_command(block_id_to_replicate: str, target_datanode_address: str):
    """
    Maneja la lógica de leer un bloque localmente y enviarlo a otro DataNode para replicación.
    """
    if block_store_instance is None:
        print(f"DataNode [{THIS_DATANODE_ID}] ERROR REPL: BlockStore no inicializado para replicar {block_id_to_replicate}.")
        return

    print(f"DataNode [{THIS_DATANODE_ID}]: Iniciando replicación del bloque {block_id_to_replicate} hacia {target_datanode_address}.")
    
    # 1. Leer el bloque del almacenamiento local.
    block_data = block_store_instance.read_block(block_id_to_replicate)
    if block_data is None:
        print(f"DataNode [{THIS_DATANODE_ID}] ERROR REPL: No se pudo leer el bloque {block_id_to_replicate} del almacenamiento local para replicación.")
        return

    # 2. Conectarse al DataNode destino y enviar el bloque.
    #    El DataNode destino usará su RPC 'ReplicateBlockToFollower'.
    try:
        async with grpc.aio.insecure_channel(target_datanode_address) as channel:
            stub = dfs_pb2_grpc.DataNodeOperationsStub(channel)
            
            # Se necesita la información completa del bloque (BlockInfo).
            # El NameNode, al enviar el comando REPLICATE_BLOCK, podría incluir más metadatos del bloque
            # si son necesarios para el 'ReplicateBlockToFollowerRequest' (ej. file_id, block_index).
            # Por ahora, asumimos que el block_id es suficiente para el destino, o que
            # el comando del NameNode incluiría estos detalles.
            # Si el .proto para ReplicateBlockToFollowerRequest requiere BlockInfo,
            # el NameNode debería proveer esa información en el comando.
            # Aquí crearemos un BlockInfo simple.
            # ¡ESTO ES UNA SIMPLIFICACIÓN! El NameNode debería proveer los metadatos completos del bloque.
            block_info_for_replication = dfs_pb2.BlockInfo(
                block_id=block_id_to_replicate,
                # file_id, block_index, block_size serían idealmente proporcionados por el NameNode
                # en el comando de replicación si el receptor los necesita para algo más que solo almacenar por block_id.
                # El tamaño sí es importante:
                block_size=len(block_data)
            )

            replication_request = dfs_pb2.ReplicateBlockToFollowerRequest(
                block_info=block_info_for_replication,
                data=block_data
            )
            
            print(f"DataNode [{THIS_DATANODE_ID}]: Enviando bloque {block_id_to_replicate} ({len(block_data)} bytes) a {target_datanode_address} para replicación.")
            # Timeout para la operación de replicación
            response = await stub.ReplicateBlockToFollower(replication_request, timeout=60) # Timeout más largo para transferencia de datos

            if response.success:
                print(f"DataNode [{THIS_DATANODE_ID}]: Replicación del bloque {block_id_to_replicate} a {target_datanode_address} completada exitosamente.")
            else:
                print(f"DataNode [{THIS_DATANODE_ID}] ERROR REPL: El DataNode destino {target_datanode_address} falló al replicar el bloque {block_id_to_replicate}. Mensaje: {response.message}")

    except grpc.aio.AioRpcError as e:
        print(f"DataNode [{THIS_DATANODE_ID}] ERROR REPL: Falla de gRPC al intentar replicar el bloque {block_id_to_replicate} a {target_datanode_address}. Código: {e.code()}, Detalles: {e.details()}")
    except Exception as e_generic:
        print(f"DataNode [{THIS_DATANODE_ID}] ERROR REPL: Excepción inesperada durante la replicación del bloque {block_id_to_replicate} a {target_datanode_address}: {e_generic}")


# La función run_heartbeat_sender es la que se importa y se ejecuta como una tarea en main_datanode.py