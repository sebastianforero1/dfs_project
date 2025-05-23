# client_dfs_sdk.py en el directorio client

import requests
import grpc
import os
import math
import json

# Importar las configuraciones del cliente
from . import config_client as cfg_sdk # Usamos cfg_sdk para evitar confusión con cfg de otros componentes

# Importar los stubs generados por gRPC
import dfs_service_pb2
import dfs_service_pb2_grpc

class DFSClientSDK:
    def __init__(self, 
                 namenode_rest_url: str = None, 
                 http_timeout: int = None, 
                 grpc_timeout: int = None):
        """
        Inicializa el SDK del cliente DFS.

        Args:
            namenode_rest_url (str, optional): La URL base de la API REST del NameNode.
                                               Si es None, usa el valor de config_client.py.
            http_timeout (int, optional): Timeout en segundos para las solicitudes HTTP.
                                          Si es None, usa el valor de config_client.py.
            grpc_timeout (int, optional): Timeout en segundos para las operaciones gRPC.
                                          Si es None, usa el valor de config_client.py.
        """
        self.namenode_url = namenode_rest_url if namenode_rest_url is not None else cfg_sdk.NAMENODE_REST_URL
        self.http_timeout = http_timeout if http_timeout is not None else cfg_sdk.DEFAULT_HTTP_TIMEOUT_SECONDS
        self.grpc_block_op_timeout = grpc_timeout if grpc_timeout is not None else cfg_sdk.DEFAULT_GRPC_BLOCK_OP_TIMEOUT_SECONDS
        
        self.current_dfs_path = "/"
        print(f"DFSClientSDK: Inicializado. Conectando a NameNode en: {self.namenode_url}")
        print(f"DFSClientSDK: HTTP Timeout: {self.http_timeout}s, gRPC Block Op Timeout: {self.grpc_block_op_timeout}s")

    def _make_namenode_request(self, method: str, endpoint: str, params: dict = None, json_data: dict = None, stream_response: bool = False):
        """
        Método de ayuda para realizar solicitudes HTTP al NameNode.
        (El cuerpo de este método es idéntico al proporcionado anteriormente, solo se asegura que usa self.http_timeout)
        """
        full_url = f"{self.namenode_url.rstrip('/')}/{endpoint.lstrip('/')}"
        try:
            response = requests.request(
                method=method.upper(),
                url=full_url,
                params=params,
                json=json_data,
                timeout=self.http_timeout, # Usa el timeout configurado
                stream=stream_response
            )
            response.raise_for_status()
            if stream_response:
                return response
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError:
                return response.text
        except requests.exceptions.HTTPError as e:
            error_detail = f"Error HTTP del NameNode: {e.response.status_code}."
            try:
                error_content = e.response.json()
                if "detail" in error_content: error_detail += f" Detalle: {error_content['detail']}"
                elif "message" in error_content: error_detail += f" Mensaje: {error_content['message']}"
                else: error_detail += f" Contenido: {e.response.text[:200]}"
            except requests.exceptions.JSONDecodeError:  error_detail += f" Respuesta no JSON: {e.response.text[:200]}"
            print(f"DFSClientSDK ERROR: {error_detail}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"DFSClientSDK ERROR: Falla en la solicitud al NameNode ({full_url}): {e}")
            return None
    
    def _resolve_dfs_path(self, dfs_path: str) -> str:
        """
        Resuelve una ruta DFS a una ruta absoluta normalizada.
        (Idéntico al proporcionado anteriormente)
        """
        if os.path.isabs(dfs_path):
            return os.path.normpath(dfs_path)
        else:
            return os.path.normpath(os.path.join(self.current_dfs_path, dfs_path))

    # --- Métodos para Operaciones de Metadatos del Sistema de Archivos ---
    # (mkdir, ls, rmdir, rm, cd, pwd son idénticos a los proporcionados anteriormente)
    # Solo se muestra mkdir como ejemplo de que no cambia su lógica interna.

    def mkdir(self, dfs_path: str) -> bool:
        resolved_path = self._resolve_dfs_path(dfs_path)
        print(f"DFSClientSDK: Creando directorio '{resolved_path}'...")
        response = self._make_namenode_request("POST", "/fs/mkdir", json_data={"path": resolved_path})
        if response and response.get("success"):
            print(f"DFSClientSDK: {response.get('message', 'Directorio creado exitosamente.')}")
            return True
        return False

    def ls(self, dfs_path: str = ".") -> list | None:
        resolved_path = self._resolve_dfs_path(dfs_path)
        print(f"DFSClientSDK: Listando contenido de '{resolved_path}'...")
        response = self._make_namenode_request("GET", "/fs/ls", params={"path": resolved_path})
        if response and response.get("success"):
            print(f"DFSClientSDK: {response.get('message')}")
            if response.get("listing"):
                for item in response["listing"]:
                    size_str = f"{item.get('size', '--'):>10}" if item['type'] == 'file' else f"{'--':>10}"
                    type_str = f"[{item['type'].upper()}]"
                    print(f"  {type_str:<7} {size_str}  {item['name']}")
            else:
                print("  (Directorio vacío o sin listado proporcionado)")
            return response.get("listing")
        return None

    def rmdir(self, dfs_path: str) -> bool:
        resolved_path = self._resolve_dfs_path(dfs_path)
        print(f"DFSClientSDK: Eliminando directorio '{resolved_path}'...")
        response = self._make_namenode_request("POST", "/fs/rmdir", json_data={"path": resolved_path})
        if response and response.get("success"):
            print(f"DFSClientSDK: {response.get('message', 'Directorio eliminado exitosamente.')}")
            return True
        return False

    def rm(self, dfs_filepath: str) -> bool:
        resolved_path = self._resolve_dfs_path(dfs_filepath)
        print(f"DFSClientSDK: Eliminando archivo '{resolved_path}'...")
        response = self._make_namenode_request("DELETE", "/fs/rm", params={"path": resolved_path})
        if response and response.get("success"):
            print(f"DFSClientSDK: {response.get('message', 'Archivo eliminado exitosamente.')}")
            return True
        return False
        
    def cd(self, dfs_path: str) -> bool:
        new_resolved_path = self._resolve_dfs_path(dfs_path)
        self.current_dfs_path = new_resolved_path
        print(f"DFSClientSDK: Directorio DFS actual cambiado a '{self.current_dfs_path}'.")
        return True

    def pwd(self) -> str:
        return self.current_dfs_path

    # --- Métodos para Operaciones de Archivos (PUT/GET) ---
    # (put y get son idénticos a los proporcionados anteriormente,
    #  solo asegurándose de que usen self.grpc_block_op_timeout para las llamadas gRPC)

    def put(self, local_filepath: str, dfs_filepath: str) -> bool:
        if not os.path.exists(local_filepath): # ... (código idéntico)
            print(f"DFSClientSDK ERROR: Archivo local '{local_filepath}' no encontrado.")
            return False
        if not os.path.isfile(local_filepath):
            print(f"DFSClientSDK ERROR: La ruta local '{local_filepath}' no es un archivo.")
            return False

        resolved_dfs_path = self._resolve_dfs_path(dfs_filepath)
        filesize = os.path.getsize(local_filepath)
        print(f"DFSClientSDK: Iniciando subida de '{local_filepath}' ({filesize} bytes) a '{resolved_dfs_path}'...")

        init_payload = {"filepath": resolved_dfs_path, "filesize": filesize}
        init_response = self._make_namenode_request("POST", "/file/initiate_put", json_data=init_payload)
        if not init_response or not init_response.get("success"):
            print(f"DFSClientSDK ERROR: Fallo al iniciar la subida con NameNode. {init_response.get('message', '') if init_response else ''}")
            return False
        
        file_id = init_response["file_id"]
        block_size_from_nn = init_response["block_size"]
        num_blocks = init_response["num_blocks"]
        print(f"DFSClientSDK: Subida iniciada por NameNode. File ID: {file_id}, BlockSize: {block_size_from_nn}, NumBlocks: {num_blocks}")

        client_reported_blocks_metadata = []

        try:
            with open(local_filepath, 'rb') as f_in:
                for i in range(num_blocks):
                    # ... (lógica de lectura de bloque idéntica)
                    print(f"DFSClientSDK: Procesando bloque {i+1}/{num_blocks}...")
                    block_content = f_in.read(block_size_from_nn)
                    current_block_actual_size = len(block_content)
                    if filesize == 0 and current_block_actual_size == 0 and i == 0: pass
                    elif current_block_actual_size == 0 and filesize > 0:
                         print(f"DFSClientSDK ERROR: Se leyó un bloque de 0 bytes del archivo local '{local_filepath}' para el bloque {i+1}, pero el archivo no está vacío.")
                         raise IOError("Error de lectura de bloque vacío inesperado.")

                    alloc_params = {"file_id": file_id, "block_index": i, "block_size_actual": current_block_actual_size}
                    alloc_response = self._make_namenode_request("GET", "/file/allocate_block", params=alloc_params)
                    if not alloc_response or not alloc_response.get("success"): # ... (manejo de error idéntico)
                        print(f"DFSClientSDK ERROR: Fallo al obtener asignación de bloque {i} de NameNode. {alloc_response.get('message', '') if alloc_response else ''}")
                        raise Exception("Fallo en asignación de bloque por NameNode.")

                    block_id = alloc_response["block_id"]
                    primary_dn_info = alloc_response["primary_datanode"]
                    follower_dns_info = alloc_response.get("follower_datanodes", [])
                    # ... (lógica de preparación de gRPC idéntica)
                    print(f"  Bloque {i+1} (ID: {block_id}), Primario: {primary_dn_info['id']} ({primary_dn_info['address']})")
                    dn_followers_for_grpc = [ dfs_service_pb2.DataNodeLocation(datanode_id=f['id'], datanode_address=f['address']) for f in follower_dns_info]
                    block_info_for_dn = dfs_service_pb2.BlockInfo(block_id=block_id, file_id=file_id, block_index=i, block_size=current_block_actual_size)
                    write_block_req = dfs_service_pb2.WriteBlockRequest(block_info=block_info_for_dn, data=block_content, followers_for_replication=dn_followers_for_grpc)
                    
                    try:
                        with grpc.insecure_channel(primary_dn_info["address"]) as channel:
                            stub = dfs_service_pb2_grpc.DataNodeOperationsStub(channel)
                            # Usar el timeout de gRPC configurado
                            dn_response = stub.WriteBlock(write_block_req, timeout=self.grpc_block_op_timeout)
                            if not dn_response.success: # ... (manejo de error idéntico)
                                print(f"DFSClientSDK ERROR: DataNode primario {primary_dn_info['id']} falló al escribir el bloque {block_id}: {dn_response.message}")
                                raise Exception(f"Fallo en escritura de bloque en DataNode primario: {dn_response.message}")
                    except grpc.RpcError as e: # ... (manejo de error idéntico)
                        print(f"DFSClientSDK ERROR: Falla de gRPC al escribir bloque {block_id} a {primary_dn_info['address']}: {e.code()} - {e.details()}")
                        raise Exception(f"Falla gRPC en escritura de bloque: {e.details()}") from e
                    
                    client_reported_blocks_metadata.append({"block_id": block_id, "block_index": i, "size": current_block_actual_size})
            
            finalize_payload = {"file_id": file_id, "success": True, "block_metadata": client_reported_blocks_metadata}
            finalize_response = self._make_namenode_request("POST", "/file/finalize_put", json_data=finalize_payload)
            if finalize_response and finalize_response.get("success"): # ... (lógica idéntica)
                print(f"DFSClientSDK: Archivo '{local_filepath}' subido exitosamente a '{resolved_dfs_path}'. {finalize_response.get('message')}")
                return True
            else: # ... (lógica idéntica)
                print(f"DFSClientSDK ERROR: Fallo al finalizar la subida con NameNode (después de escribir bloques). {finalize_response.get('message', '') if finalize_response else ''}")
                return False
        except Exception as e: # ... (lógica idéntica)
            print(f"DFSClientSDK ERROR: Ocurrió un error durante la operación 'put' para '{local_filepath}': {e}")
            finalize_payload_failure = {"file_id": file_id, "success": False, "block_metadata": client_reported_blocks_metadata}
            self._make_namenode_request("POST", "/file/finalize_put", json_data=finalize_payload_failure)
            return False

    def get(self, dfs_filepath: str, local_save_path: str) -> bool:
        resolved_dfs_path = self._resolve_dfs_path(dfs_filepath) # ... (código idéntico al anterior para get)
        print(f"DFSClientSDK: Iniciando descarga de '{resolved_dfs_path}' a '{local_save_path}'...")

        info_params = {"filepath": resolved_dfs_path}
        file_info_response = self._make_namenode_request("GET", "/file/info_for_get", params=info_params)

        if not file_info_response or not file_info_response.get("success"): # ... (manejo de error idéntico)
            print(f"DFSClientSDK ERROR: No se pudo obtener información del archivo '{resolved_dfs_path}' desde NameNode. {file_info_response.get('message', '') if file_info_response else ''}")
            return False

        blocks_to_read_meta = sorted(file_info_response.get("blocks", []), key=lambda b: b["block_index"])
        if not blocks_to_read_meta and file_info_response.get("total_size", -1) > 0 : # ... (manejo de error idéntico)
            print(f"DFSClientSDK ERROR: NameNode reportó {file_info_response.get('total_size')} bytes pero no hay información de bloques para '{resolved_dfs_path}'.")
            return False
        elif not blocks_to_read_meta and file_info_response.get("total_size", -1) == 0: # ... (manejo de archivo vacío idéntico)
             print(f"DFSClientSDK: El archivo '{resolved_dfs_path}' está vacío. Creando archivo local vacío.")
             try:
                with open(local_save_path, 'wb') as f_out_empty: pass
                return True
             except IOError as e:
                print(f"DFSClientSDK ERROR: No se pudo crear el archivo local vacío '{local_save_path}': {e}")
                return False

        print(f"DFSClientSDK: Archivo '{resolved_dfs_path}' tiene {len(blocks_to_read_meta)} bloques. Tamaño total: {file_info_response.get('total_size', 'N/A')} bytes.")
        try:
            with open(local_save_path, 'wb') as f_out:
                for block_meta in blocks_to_read_meta: # ... (lógica de iteración idéntica)
                    block_id = block_meta["block_id"]
                    block_index = block_meta["block_index"]
                    expected_block_size = block_meta["size"]
                    locations = block_meta.get("locations", [])
                    if not locations: # ... (manejo de error idéntico)
                        print(f"DFSClientSDK ERROR: No hay ubicaciones de DataNode activas para el bloque {block_id} (índice {block_index}). Descarga abortada.")
                        raise Exception(f"Bloque {block_id} inaccesible.")
                    print(f"  Leyendo bloque {block_index + 1}/{len(blocks_to_read_meta)} (ID: {block_id}, Tamaño esp: {expected_block_size} bytes)...")
                    block_data_read = None
                    last_dn_error = None
                    for dn_loc_info in locations: # ... (lógica de intento de lectura idéntica)
                        dn_address = dn_loc_info["address"]
                        dn_id_str = dn_loc_info["id"]
                        try:
                            with grpc.insecure_channel(dn_address) as channel:
                                stub = dfs_service_pb2_grpc.DataNodeOperationsStub(channel)
                                read_req = dfs_service_pb2.ReadBlockRequest(block_id=block_id)
                                # Usar el timeout de gRPC configurado
                                dn_response = stub.ReadBlock(read_req, timeout=self.grpc_block_op_timeout)
                                if dn_response.success and dn_response.data is not None: # ... (lógica de éxito idéntica)
                                    block_data_read = dn_response.data
                                    if len(block_data_read) != expected_block_size:
                                        print(f"DFSClientSDK WARNING: Tamaño de bloque leído ({len(block_data_read)}) para {block_id} de {dn_id_str} no coincide con el esperado ({expected_block_size}). Usando tamaño leído.")
                                    break
                                else: # ... (manejo de error de DN idéntico)
                                    last_dn_error = f"DataNode {dn_id_str} falló al proveer el bloque {block_id}: {dn_response.message}"
                                    print(f"DFSClientSDK WARNING: {last_dn_error}")
                        except grpc.RpcError as e: # ... (manejo de error gRPC idéntico)
                            last_dn_error = f"Falla de gRPC al leer bloque {block_id} de {dn_address}: {e.code()} - {e.details()}"
                            print(f"DFSClientSDK WARNING: {last_dn_error}")
                        except Exception as e_inner: # ... (manejo de error interno idéntico)
                            last_dn_error = f"Excepción interna al leer bloque de {dn_address}: {e_inner}"
                            print(f"DFSClientSDK WARNING: {last_dn_error}")
                    if block_data_read is not None: # ... (escritura de bloque idéntica)
                        f_out.write(block_data_read)
                    else: # ... (fallo de lectura de todas las fuentes idéntico)
                        print(f"DFSClientSDK ERROR: Fallo al leer el bloque {block_id} de todas las ubicaciones disponibles. Último error: {last_dn_error}. Descarga abortada.")
                        raise Exception(f"Fallo en lectura de bloque {block_id}.")
            print(f"DFSClientSDK: Archivo '{resolved_dfs_path}' descargado exitosamente a '{local_save_path}'.")
            return True
        except Exception as e: # ... (manejo de excepción y limpieza idéntico)
            print(f"DFSClientSDK ERROR: Ocurrió un error durante la operación 'get' para '{resolved_dfs_path}': {e}")
            if os.path.exists(local_save_path):
                try: os.remove(local_save_path); print(f"DFSClientSDK: Archivo local parcial '{local_save_path}' eliminado.")
                except OSError as e_del: print(f"DFSClientSDK ERROR: No se pudo eliminar el archivo local parcial '{local_save_path}': {e_del}")
            return False