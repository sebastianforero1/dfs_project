# grpc_services.py en el directorio namenode

import grpc

# Importaciones de los stubs generados por gRPC
# Asegúrate de que dfs_service_pb2.py y dfs_service_pb2_grpc.py
# estén en la raíz del proyecto o en una ubicación accesible por PYTHONPATH.
import dfs_pb2 
import dfs_pb2_grpc

# Importar la instancia de MetadataManager (asumiendo que se inyectará o será accesible)
# Esta es una referencia que se establecerá en main_namenode.py
metadata_manager = None # Se asignará desde main_namenode.py

class NameNodeManagementServicer(dfs_pb2_grpc.NameNodeManagementServicer):
    """
    Implementación del servicio gRPC 'NameNodeManagement'.
    Este servicio es utilizado por los DataNodes para enviar heartbeats
    y reportes de bloques al NameNode.
    """

    async def SendHeartbeat(self, request: dfs_pb2.HeartbeatRequest, context: grpc.aio.ServicerContext):
        """
        Procesa un mensaje de heartbeat enviado por un DataNode.

        Args:
            request (dfs_pb2.HeartbeatRequest): La solicitud de heartbeat del DataNode.
                Contiene:
                - datanode_info (DataNodeLocation): ID y dirección del DataNode.
                - block_ids_stored (repeated string): Lista de IDs de bloques que el DataNode almacena.
                - available_storage_bytes (uint64): Espacio de almacenamiento disponible en el DataNode.
            context (grpc.aio.ServicerContext): Contexto de la llamada gRPC.

        Returns:
            dfs_pb2.HeartbeatResponse: Una respuesta que puede incluir comandos
            para el DataNode (ej. eliminar bloques, replicar bloques).
        """
        if metadata_manager is None:
            # Este error es crítico y no debería ocurrir en un sistema bien inicializado.
            # Se informa al DataNode que el servicio no está disponible.
            error_message = "NameNode: MetadataManager no inicializado. No se puede procesar el heartbeat."
            print(f"ERROR CRÍTICO: {error_message}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(error_message)
            return dfs_pb2.HeartbeatResponse()

        datanode_id = request.datanode_info.datanode_id
        datanode_address = request.datanode_info.datanode_address
        reported_block_ids = list(request.block_ids_stored) # Convertir a lista de Python
        available_storage = request.available_storage_bytes

        # print(f"NameNode gRPC: Heartbeat recibido de DataNode ID: {datanode_id}, Addr: {datanode_address}, Bloques: {len(reported_block_ids)}, Espacio: {available_storage // (1024*1024)}MB")

        try:
            # Delegar el procesamiento del heartbeat al MetadataManager
            commands_for_datanode = metadata_manager.process_datanode_heartbeat(
                datanode_id=datanode_id,
                datanode_address=datanode_address,
                reported_block_ids=reported_block_ids,
                available_storage_bytes=available_storage
            )

            # Devolver los comandos (si los hay) al DataNode
            return dfs_pb2.HeartbeatResponse(commands_from_namenode=commands_for_datanode)

        except Exception as e:
            # Manejar cualquier excepción inesperada durante el procesamiento del heartbeat.
            error_message = f"NameNode: Error al procesar el heartbeat de {datanode_id}: {e}"
            print(f"ERROR: {error_message}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_message)
            return dfs_pb2.HeartbeatResponse()

# Si hubiera otros servicios gRPC que el NameNode expone (por ejemplo, para la sincronización
# con un NameNode seguidor), sus implementaciones (Servicers) se añadirían aquí.
# Ejemplo:
# class NameNodeSyncServicer(dfs_pb2_grpc.NameNodeSyncServicer):
#     async def SyncMetadata(self, request_iterator, context):
#         # Implementación de la lógica de sincronización
#         pass