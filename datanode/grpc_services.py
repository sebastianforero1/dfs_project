# grpc_services.py en el directorio datanode

import grpc
import asyncio # Para la replicación asíncrona si se decide implementar así

# Importaciones de los stubs generados por gRPC
# Asegúrate de que dfs_service_pb2.py y dfs_service_pb2_grpc.py
# estén en la raíz del proyecto o en una ubicación accesible por PYTHONPATH.
import dfs_service_pb2
import dfs_service_pb2_grpc

# --- Variables de Configuración/Instancias (serán establecidas por main_datanode.py) ---
# Estas variables se configuran mediante "inyección" desde main_datanode.py
# para que este módulo no dependa directamente de os.environ o de una instancia global fija.

block_store_instance = None # Instancia de BlockStore para las operaciones de disco
THIS_DATANODE_ID = "datanode_unconfigured_grpc"
THIS_DATANODE_GRPC_ADDRESS = "localhost:0" # Dirección que este DataNode anuncia (para logging)

class DataNodeOperationsServicer(dfs_service_pb2_grpc.DataNodeOperationsServicer):
    """
    Implementación del servicio gRPC 'DataNodeOperations'.
    Este servicio maneja las operaciones de lectura, escritura y replicación de bloques.
    """

    async def WriteBlock(self, request: dfs_service_pb2.WriteBlockRequest, context: grpc.aio.ServicerContext):
        """
        RPC para que un cliente escriba un bloque en este DataNode.
        Si este DataNode es el primario para el bloque, también se encarga de
        coordinar la replicación a los DataNodes seguidores especificados.
        """
        if block_store_instance is None:
            error_message = f"DataNode [{THIS_DATANODE_ID}] ERROR: BlockStore no inicializado. No se puede procesar WriteBlock."
            print(error_message)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(error_message)
            return dfs_service_pb2.WriteBlockResponse(success=False, message=error_message)

        block_info = request.block_info
        block_id = block_info.block_id
        data_to_write = request.data
        followers_for_replication = request.followers_for_replication

        # print(f"DataNode [{THIS_DATANODE_ID}]: Solicitud WriteBlock recibida para ID: {block_id}, Tamaño: {len(data_to_write)} bytes. Seguidores para replicar: {len(followers_for_replication)}")

        # 1. Escribir el bloque localmente.
        write_success_local = block_store_instance.write_block(block_id, data_to_write)

        if not write_success_local:
            error_message = f"DataNode [{THIS_DATANODE_ID}]: Fallo al escribir el bloque {block_id} localmente."
            print(error_message)
            context.set_code(grpc.StatusCode.INTERNAL) # Error interno del servidor
            context.set_details(error_message)
            return dfs_service_pb2.WriteBlockResponse(success=False, message=error_message)

        # 2. Si la escritura local fue exitosa, y hay seguidores, replicar el bloque.
        #    Este DataNode actúa como "líder del bloque" para esta escritura.
        replication_messages = []
        all_replications_successful = True

        if followers_for_replication:
            # print(f"DataNode [{THIS_DATANODE_ID}]: Iniciando replicación del bloque {block_id} a {len(followers_for_replication)} seguidores.")
            
            # Se podrían ejecutar las replicaciones en paralelo usando asyncio.gather
            replication_tasks = []
            for follower_info in followers_for_replication:
                # Asegurarse de no intentar replicar a sí mismo si por error se incluye
                if follower_info.datanode_id == THIS_DATANODE_ID or follower_info.datanode_address == THIS_DATANODE_GRPC_ADDRESS:
                    # print(f"DataNode [{THIS_DATANODE_ID}]: Omitiendo auto-replicación para el bloque {block_id}.")
                    continue
                
                replication_tasks.append(
                    self._replicate_block_to_single_follower(block_id, data_to_write, block_info, follower_info)
                )
            
            if replication_tasks:
                # Esperar a que todas las tareas de replicación (a cada seguidor) se completen
                results = await asyncio.gather(*replication_tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    follower_dn_id = followers_for_replication[i].datanode_id # Asumiendo el orden se mantiene
                    if isinstance(result, Exception):
                        msg = f"Excepción al replicar a {follower_dn_id}: {result}"
                        print(f"DataNode [{THIS_DATANODE_ID}] ERROR REPL: {msg}")
                        replication_messages.append(msg)
                        all_replications_successful = False
                    elif not result.get("success"):
                        msg = f"Fallo al replicar a {follower_dn_id}: {result.get('message', 'Error desconocido del seguidor.')}"
                        print(f"DataNode [{THIS_DATANODE_ID}] ERROR REPL: {msg}")
                        replication_messages.append(msg)
                        all_replications_successful = False
                    else:
                        msg = f"Replicación a {follower_dn_id} exitosa."
                        # print(f"DataNode [{THIS_DATANODE_ID}]: {msg}")
                        replication_messages.append(msg)
        
        # Construir el mensaje de respuesta final
        final_message = f"Bloque {block_id} escrito localmente en {THIS_DATANODE_ID}."
        if replication_messages:
            final_message += " Resultados de replicación: " + "; ".join(replication_messages)
        
        # El proyecto especifica que la replicación debe garantizarse "en todo momento".
        # Si la replicación a un seguidor falla, ¿debería fallar toda la operación WriteBlock?
        # Por ahora, si la escritura local fue exitosa pero alguna replicación falló,
        # aún devolvemos éxito al cliente, pero con un mensaje detallado.
        # El NameNode eventualmente detectará sub-replicación y la corregirá.
        # Sin embargo, para un sistema WORM estricto con replicación síncrona, se podría fallar si `all_replications_successful` es False.
        # Para este proyecto, priorizaremos que la escritura al primario sea el indicador de éxito para el cliente.
        
        return dfs_service_pb2.WriteBlockResponse(success=True, message=final_message)

    async def _replicate_block_to_single_follower(self, block_id: str, data: bytes, original_block_info: dfs_service_pb2.BlockInfo, follower_info: dfs_service_pb2.DataNodeLocation) -> dict:
        """
        Función de ayuda para replicar un bloque a un único DataNode seguidor.
        """
        # print(f"DataNode [{THIS_DATANODE_ID}]: Intentando replicar bloque {block_id} a seguidor {follower_info.datanode_id} en {follower_info.datanode_address}")
        try:
            async with grpc.aio.insecure_channel(follower_info.datanode_address) as channel:
                stub = dfs_service_pb2_grpc.DataNodeOperationsStub(channel)
                
                # El seguidor solo necesita almacenar el bloque, no necesita replicar más allá.
                # Se usa la información original del bloque (file_id, index, size) que vino del cliente.
                replication_req = dfs_service_pb2.ReplicateBlockToFollowerRequest(
                    block_info=original_block_info, # Pasar la BlockInfo original
                    data=data
                )
                
                # Timeout para la llamada de replicación (puede ser más largo por transferencia de datos)
                response = await stub.ReplicateBlockToFollower(replication_req, timeout=60) # Ej: 60 segundos
                
                if response.success:
                    return {"success": True, "message": f"Réplica de {block_id} enviada a {follower_info.datanode_id} exitosamente."}
                else:
                    return {"success": False, "message": f"Seguidor {follower_info.datanode_id} falló al almacenar réplica de {block_id}: {response.message}"}
        
        except grpc.aio.AioRpcError as e:
            return {"success": False, "message": f"Error gRPC al replicar {block_id} a {follower_info.datanode_id} ({follower_info.datanode_address}): {e.code()} - {e.details()}"}
        except Exception as e_generic:
            return {"success": False, "message": f"Excepción general al replicar {block_id} a {follower_info.datanode_id}: {e_generic}"}


    async def ReadBlock(self, request: dfs_service_pb2.ReadBlockRequest, context: grpc.aio.ServicerContext):
        """
        RPC para que un cliente lea un bloque desde este DataNode.
        """
        if block_store_instance is None:
            error_message = f"DataNode [{THIS_DATANODE_ID}] ERROR: BlockStore no inicializado. No se puede procesar ReadBlock."
            print(error_message)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(error_message)
            return dfs_service_pb2.ReadBlockResponse(data=b"", success=False, message=error_message)

        block_id_to_read = request.block_id
        # print(f"DataNode [{THIS_DATANODE_ID}]: Solicitud ReadBlock recibida para ID: {block_id_to_read}")

        block_data = block_store_instance.read_block(block_id_to_read)

        if block_data is not None:
            # print(f"DataNode [{THIS_DATANODE_ID}]: Bloque {block_id_to_read} leído exitosamente (tamaño: {len(block_data)} bytes).")
            return dfs_service_pb2.ReadBlockResponse(data=block_data, success=True, message="Bloque leído exitosamente.")
        else:
            error_message = f"DataNode [{THIS_DATANODE_ID}]: Bloque {block_id_to_read} no encontrado o no se pudo leer."
            # print(error_message)
            context.set_code(grpc.StatusCode.NOT_FOUND) # El recurso (bloque) no fue encontrado
            context.set_details(error_message)
            return dfs_service_pb2.ReadBlockResponse(data=b"", success=False, message=error_message)


    async def ReplicateBlockToFollower(self, request: dfs_service_pb2.ReplicateBlockToFollowerRequest, context: grpc.aio.ServicerContext):
        """
        RPC para que otro DataNode (actuando como líder de bloque) envíe una réplica
        de un bloque para que este DataNode (actuando como seguidor) la almacene.
        """
        if block_store_instance is None:
            error_message = f"DataNode [{THIS_DATANODE_ID}] ERROR: BlockStore no inicializado. No se puede procesar ReplicateBlockToFollower."
            print(error_message)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(error_message)
            return dfs_service_pb2.ReplicateBlockToFollowerResponse(success=False, message=error_message)

        block_info = request.block_info
        block_id = block_info.block_id
        data_to_store = request.data

        # print(f"DataNode [{THIS_DATANODE_ID}] (como Seguidor): Solicitud ReplicateBlockToFollower recibida para ID: {block_id}, Tamaño: {len(data_to_store)} bytes.")

        # Escribir el bloque localmente (como seguidor).
        write_success = block_store_instance.write_block(block_id, data_to_store)

        if write_success:
            # print(f"DataNode [{THIS_DATANODE_ID}] (como Seguidor): Réplica del bloque {block_id} almacenada exitosamente.")
            return dfs_service_pb2.ReplicateBlockToFollowerResponse(success=True, message="Réplica del bloque almacenada exitosamente.")
        else:
            error_message = f"DataNode [{THIS_DATANODE_ID}] (como Seguidor): Fallo al almacenar la réplica del bloque {block_id}."
            print(error_message)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_message)
            return dfs_service_pb2.ReplicateBlockToFollowerResponse(success=False, message=error_message)

    # Si se implementa un comando directo del NameNode para eliminar bloques (en lugar de vía heartbeat):
    # async def DeleteBlocks(self, request: dfs_service_pb2.DeleteBlocksRequest, context):
    #     """ RPC para que el NameNode instruya la eliminación de bloques. """
    #     if block_store_instance is None: # ... (manejo de error)
    #         pass
    #     print(f"DataNode [{THIS_DATANODE_ID}]: Solicitud DeleteBlocks recibida para: {list(request.block_ids_to_delete)}")
    #     success_count = 0
    #     fail_count = 0
    #     messages = []
    #     for block_id_del in request.block_ids_to_delete:
    #         if block_store_instance.delete_block(block_id_del):
    #             success_count +=1
    #         else:
    #             fail_count +=1
    #             messages.append(f"Fallo al borrar {block_id_del}")
        
    #     final_msg = f"Eliminados {success_count} bloques, fallaron {fail_count}."
    #     if messages: final_msg += " Errores: " + "; ".join(messages)
        
    #     return dfs_service_pb2.DeleteBlocksResponse(success=(fail_count == 0), message=final_msg)