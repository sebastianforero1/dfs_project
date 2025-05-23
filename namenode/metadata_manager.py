# metadata_manager.py en el directorio namenode

import threading
import json
import os
import time # Para los timestamps de los heartbeats
import random
import uuid # Para generar IDs únicos de archivo y bloque
from datetime import datetime, timedelta # Para el timeout de los DataNodes

class MetadataManager:
    def __init__(self, config):
        """
        Inicializa el MetadataManager.
        
        Args:
            config (dict): Un diccionario de configuración que contiene:
                - BLOCK_SIZE (int): Tamaño de cada bloque de archivo en bytes.
                - REPLICATION_FACTOR (int): Número deseado de réplicas por bloque.
                - DATANODE_TIMEOUT_SECONDS (int): Tiempo en segundos para considerar un DataNode inactivo.
                - NAMENODE_METADATA_FILE (str): Ruta al archivo donde se persistirán los metadatos.
        """
        self.config = config
        self.metadata_file_path = self.config.get("NAMENODE_METADATA_FILE", "namenode_metadata_snapshot.json")
        self.block_size = self.config.get("BLOCK_SIZE", 64 * 1024 * 1024) # Por defecto 64MB
        self.default_replication_factor = self.config.get("REPLICATION_FACTOR", 2) # Por defecto 2 réplicas
        self.datanode_timeout_seconds = self.config.get("DATANODE_TIMEOUT_SECONDS", 30) # Por defecto 30 segundos

        # Estructuras de datos principales para los metadatos:
        # Usar un Lock para proteger el acceso concurrente a estas estructuras es crucial si
        # los métodos son llamados desde múltiples hilos (ej: múltiples requests API/gRPC).
        self.metadata_lock = threading.Lock()

        # 1. Espacio de nombres del sistema de archivos (jerarquía de directorios y archivos)
        #    Ejemplo: {"/": {"type": "dir", "children": {"usr": {"type": "dir", ...}, "file.txt": {"type": "file", "file_id": "..."}}}}
        self.fs_tree = {"/": {"type": "dir", "children": {}}}

        # 2. Mapeo de file_id a la lista ordenada de metadatos de sus bloques
        #    Ejemplo: {"file_id_123": [{"block_id": "block_A", "block_index": 0, "size": 67108864, "datanode_ids": ["dn1","dn2"]}, ...]}
        #    'datanode_ids' aquí son las ubicaciones *confirmadas* (o al menos asignadas y en proceso de confirmación).
        self.file_to_block_map = {}

        # 3. Mapeo de block_id a la lista de DataNode IDs que almacenan una réplica de ese bloque.
        #    Esto es la fuente de verdad para la ubicación de los bloques. Se actualiza con los heartbeats/reportes.
        #    Ejemplo: {"block_A": {"dn1", "dn2", "dn3"}} (usar un set para evitar duplicados y facilitar adición/remoción)
        self.block_locations = {}

        # 4. Estado de los DataNodes activos.
        #    Ejemplo: {"datanode_id_1": {"address": "ip:port", "last_heartbeat_ts": 1678886400, 
        #                                "blocks_reported": {"block_A", "block_C"}, "available_storage_bytes": 1024*1024*100}}
        self.active_datanodes = {}
        
        # 5. Bloques pendientes de replicación o eliminación (para que los chequeos periódicos actúen)
        self.pending_replication_tasks = {} # block_id -> {current_replicas, target_replicas, source_candidates}
        self.pending_deletion_blocks_on_datanodes = {} # datanode_id -> {block_id1, block_id2}

        # Cargar metadatos desde el archivo al iniciar.
        self._load_metadata_from_file()

    def _save_metadata_to_file(self):
        """Guarda el estado actual de los metadatos en un archivo JSON."""
        with self.metadata_lock:
            try:
                # Preparamos los datos para serializar. No guardamos locks ni objetos no serializables directamente.
                # active_datanodes es transitorio y se reconstruye con heartbeats, pero podemos guardar la info
                # de los bloques que reportaron por si el NameNode reinicia muy rápido.
                # Sin embargo, para un snapshot simple, nos enfocamos en fs_tree, file_to_block_map, y block_locations.
                # Los sets se convierten a listas para JSON.
                data_to_persist = {
                    "fs_tree": self.fs_tree,
                    "file_to_block_map": self.file_to_block_map,
                    "block_locations": {bid: list(dns) for bid, dns in self.block_locations.items()},
                    # 'active_datanodes' se reconstruirá con los heartbeats.
                    # 'pending_tasks' también son transitorios y se re-evaluarán.
                }
                with open(self.metadata_file_path, 'w') as f:
                    json.dump(data_to_persist, f, indent=4)
                print(f"MetadataManager: Metadatos guardados en '{self.metadata_file_path}'")
            except IOError as e:
                print(f"MetadataManager ERROR: No se pudo guardar metadatos en '{self.metadata_file_path}': {e}")

    def _load_metadata_from_file(self):
        """Carga los metadatos desde un archivo JSON al iniciar."""
        with self.metadata_lock:
            if not os.path.exists(self.metadata_file_path):
                print(f"MetadataManager: Archivo de metadatos '{self.metadata_file_path}' no encontrado. Iniciando con metadatos vacíos.")
                # Asegurar que las estructuras base existan incluso si no se carga nada
                self.fs_tree = {"/": {"type": "dir", "children": {}}}
                self.file_to_block_map = {}
                self.block_locations = {}
                return

            try:
                with open(self.metadata_file_path, 'r') as f:
                    loaded_data = json.load(f)
                    self.fs_tree = loaded_data.get("fs_tree", {"/": {"type": "dir", "children": {}}})
                    self.file_to_block_map = loaded_data.get("file_to_block_map", {})
                    # Convertir listas de vuelta a sets para block_locations
                    self.block_locations = {
                        bid: set(dns) for bid, dns in loaded_data.get("block_locations", {}).items()
                    }
                print(f"MetadataManager: Metadatos cargados desde '{self.metadata_file_path}'")
            except (IOError, json.JSONDecodeError) as e:
                print(f"MetadataManager ERROR: No se pudo cargar metadatos desde '{self.metadata_file_path}': {e}. Iniciando con metadatos vacíos.")
                # Resetear a estado por defecto si la carga falla
                self.fs_tree = {"/": {"type": "dir", "children": {}}}
                self.file_to_block_map = {}
                self.block_locations = {}

    # --- Métodos de ayuda para el espacio de nombres (fs_tree) ---
    def _get_node_by_path(self, path_str: str):
        """Obtiene un nodo (archivo o directorio) del fs_tree dada una ruta."""
        # Asume que path_str es una ruta absoluta normalizada
        parts = [part for part in path_str.split('/') if part]
        current_level = self.fs_tree["/"]
        for part in parts:
            if current_level["type"] != "dir" or part not in current_level["children"]:
                return None
            current_level = current_level["children"][part]
        return current_level

    def _get_parent_and_name_from_path(self, path_str: str):
        """Dada una ruta, devuelve el nodo padre y el nombre del último componente."""
        if path_str == "/":
            return None, "/" # El padre de la raíz es None, el nombre es "/"
        
        parts = [part for part in path_str.split('/') if part]
        if not parts: # Debería ser manejado por path_str == "/"
            return None, None

        name = parts[-1]
        parent_path_parts = parts[:-1]
        
        parent_node = self.fs_tree["/"]
        for part in parent_path_parts:
            if parent_node["type"] != "dir" or part not in parent_node["children"]:
                return None, None # Ruta padre inválida
            parent_node = parent_node["children"][part]
        
        return parent_node, name

    # --- Operaciones del Sistema de Archivos (API REST) ---

    def mkdir(self, path: str):
        """Crea un nuevo directorio."""
        with self.metadata_lock:
            # Normalizar la ruta
            norm_path = os.path.normpath(path)
            if not norm_path.startswith("/"):
                return False, "La ruta debe ser absoluta."
            if norm_path == "/":
                return False, "No se puede crear el directorio raíz (ya existe)."

            parent_node, dir_name = self._get_parent_and_name_from_path(norm_path)

            if not parent_node or parent_node["type"] != "dir":
                return False, "La ruta padre no existe o no es un directorio."
            if dir_name in parent_node["children"]:
                return False, f"Ya existe un archivo o directorio con el nombre '{dir_name}' en esta ubicación."

            parent_node["children"][dir_name] = {"type": "dir", "children": {}}
            self._save_metadata_to_file() # Persistir cambio
            return True, f"Directorio '{norm_path}' creado exitosamente."

    def ls(self, path: str):
        """Lista el contenido de un directorio."""
        with self.metadata_lock:
            norm_path = os.path.normpath(path)
            if not norm_path.startswith("/"):
                return None, "La ruta debe ser absoluta."

            node = self._get_node_by_path(norm_path)
            if not node:
                return None, f"Ruta '{norm_path}' no encontrada."
            if node["type"] != "dir":
                return None, f"'{norm_path}' no es un directorio."

            listing = []
            for name, item_meta in node["children"].items():
                entry = {"name": name, "type": item_meta["type"]}
                if item_meta["type"] == "file":
                    file_id = item_meta.get("file_id")
                    entry["file_id"] = file_id
                    # Calcular tamaño total del archivo
                    total_size = 0
                    if file_id and file_id in self.file_to_block_map:
                        for block_meta in self.file_to_block_map[file_id]:
                            total_size += block_meta.get("size", 0)
                    entry["size"] = total_size
                listing.append(entry)
            return listing, f"Listado de '{norm_path}'."
            
    def rmdir(self, path: str):
        """Elimina un directorio (solo si está vacío)."""
        with self.metadata_lock:
            norm_path = os.path.normpath(path)
            if not norm_path.startswith("/") or norm_path == "/":
                return False, "No se puede eliminar el directorio raíz o la ruta no es absoluta."

            parent_node, dir_name = self._get_parent_and_name_from_path(norm_path)
            if not parent_node or dir_name not in parent_node["children"]:
                return False, f"Directorio '{norm_path}' no encontrado."

            dir_to_delete = parent_node["children"][dir_name]
            if dir_to_delete["type"] != "dir":
                return False, f"'{norm_path}' no es un directorio."
            if dir_to_delete["children"]: # No está vacío
                return False, f"El directorio '{norm_path}' no está vacío."

            del parent_node["children"][dir_name]
            self._save_metadata_to_file()
            return True, f"Directorio '{norm_path}' eliminado."

    def rm(self, path: str):
        """Elimina un archivo y sus bloques."""
        with self.metadata_lock:
            norm_path = os.path.normpath(path)
            if not norm_path.startswith("/"):
                return False, "La ruta debe ser absoluta."

            parent_node, file_name = self._get_parent_and_name_from_path(norm_path)
            if not parent_node or file_name not in parent_node["children"]:
                return False, f"Archivo '{norm_path}' no encontrado."

            file_meta = parent_node["children"][file_name]
            if file_meta["type"] != "file":
                return False, f"'{norm_path}' no es un archivo."

            file_id = file_meta.get("file_id")
            if not file_id: # No debería pasar si está bien formado
                del parent_node["children"][file_name] # Eliminar la entrada rota
                self._save_metadata_to_file()
                return False, "Metadatos del archivo inconsistentes (sin file_id)."

            # Marcar bloques para eliminación por los DataNodes
            blocks_to_remove_from_system = []
            if file_id in self.file_to_block_map:
                for block_entry in self.file_to_block_map[file_id]:
                    block_id = block_entry["block_id"]
                    blocks_to_remove_from_system.append(block_id)
                    # Indicar a los DataNodes que eliminen este bloque
                    if block_id in self.block_locations:
                        for dn_id in list(self.block_locations[block_id]): # Copiar set para iterar
                            if dn_id not in self.pending_deletion_blocks_on_datanodes:
                                self.pending_deletion_blocks_on_datanodes[dn_id] = set()
                            self.pending_deletion_blocks_on_datanodes[dn_id].add(block_id)
                        # Ya no necesitamos rastrear este bloque para este archivo
                        del self.block_locations[block_id] 
                
                del self.file_to_block_map[file_id] # Eliminar mapeo de archivo a bloques

            # Eliminar el archivo del árbol de directorios
            del parent_node["children"][file_name]
            self._save_metadata_to_file()
            print(f"MetadataManager: Archivo '{norm_path}' (ID: {file_id}) eliminado. Bloques marcados para borrado en DataNodes: {blocks_to_remove_from_system}")
            return True, f"Archivo '{norm_path}' eliminado. Sus bloques serán purgados."


    # --- Operaciones de Escritura de Archivos ---

    def initiate_put_file(self, dfs_filepath: str, filesize: int):
        """Inicia el proceso de escritura de un archivo."""
        with self.metadata_lock:
            norm_path = os.path.normpath(dfs_filepath)
            if not norm_path.startswith("/"):
                return None, "La ruta debe ser absoluta."

            parent_node, filename = self._get_parent_and_name_from_path(norm_path)
            if not parent_node or parent_node["type"] != "dir":
                return None, "La ruta padre no existe o no es un directorio."

            # Política WORM: Si el archivo existe, la operación 'put' lo reemplaza.
            # Para reemplazar, primero eliminamos el archivo existente y sus bloques.
            if filename in parent_node["children"] and parent_node["children"][filename]["type"] == "file":
                print(f"MetadataManager: Reemplazando archivo existente en '{norm_path}'. Eliminando versión anterior...")
                # Usamos el ID del archivo existente para limpiar sus bloques
                old_file_id = parent_node["children"][filename].get("file_id")
                if old_file_id:
                    self._cleanup_file_blocks(old_file_id) # Método de ayuda para limpiar bloques
                # No es necesario llamar a rm() completo, solo limpiar metadatos y bloques asociados.
            elif filename in parent_node["children"] and parent_node["children"][filename]["type"] == "dir":
                 return None, f"Ya existe un directorio con el nombre '{filename}' en esta ubicación."


            file_id = str(uuid.uuid4())
            parent_node["children"][filename] = {
                "type": "file",
                "file_id": file_id,
                "status": "upload_in_progress", # Estado inicial
                "size": filesize # Tamaño total esperado
            }
            self.file_to_block_map[file_id] = [] # Lista para los metadatos de los bloques

            num_blocks = (filesize + self.block_size - 1) // self.block_size if filesize > 0 else 0
            if filesize == 0: num_blocks = 1 # Archivo vacío aún tiene 1 "bloque" de 0 bytes

            self._save_metadata_to_file()
            response_data = {
                "file_id": file_id,
                "block_size": self.block_size,
                "num_blocks": num_blocks
            }
            return response_data, f"Iniciada la subida para '{norm_path}', File ID: {file_id}."

    def _cleanup_file_blocks(self, file_id_to_clean: str):
        """Método interno para limpiar los bloques de un archivo que se va a reemplazar o eliminar."""
        # Similar a la lógica de rm(), pero enfocado solo en los bloques y sus metadatos
        if file_id_to_clean in self.file_to_block_map:
            for block_entry in self.file_to_block_map[file_id_to_clean]:
                block_id = block_entry["block_id"]
                if block_id in self.block_locations:
                    for dn_id in list(self.block_locations[block_id]):
                        if dn_id not in self.pending_deletion_blocks_on_datanodes:
                            self.pending_deletion_blocks_on_datanodes[dn_id] = set()
                        self.pending_deletion_blocks_on_datanodes[dn_id].add(block_id)
                    del self.block_locations[block_id]
            del self.file_to_block_map[file_id_to_clean]
        # El _save_metadata_to_file() se llamará después de que se complete la operación principal (ej. initiate_put)

    def allocate_block_locations(self, file_id: str, block_index: int, block_size_actual: int):
        """Asigna DataNodes para un nuevo bloque de archivo."""
        with self.metadata_lock:
            if file_id not in self.file_to_block_map:
                return None, "File ID no encontrado o la subida no fue iniciada."

            # Seleccionar DataNodes para este bloque
            # Necesitamos al menos self.default_replication_factor DataNodes activos
            active_dn_ids = list(self.active_datanodes.keys())
            if len(active_dn_ids) < self.default_replication_factor:
                return None, f"No hay suficientes DataNodes activos (necesarios: {self.default_replication_factor}, disponibles: {len(active_dn_ids)}) para la replicación."

            # Algoritmo de selección simple: aleatorio, pero podría ser más inteligente
            # (ej. basado en carga, espacio disponible, rack-awareness no implementado aquí)
            # Nos aseguramos de no seleccionar el mismo DataNode varias veces para el mismo bloque.
            if len(active_dn_ids) >= self.default_replication_factor:
                chosen_datanode_ids = random.sample(active_dn_ids, self.default_replication_factor)
            else:
                # No debería llegar aquí si la comprobación anterior es correcta.
                return None, "Error lógico en la selección de DataNodes."

            primary_dn_id = chosen_datanode_ids[0]
            follower_dn_ids = chosen_datanode_ids[1:] # Los restantes son seguidores

            block_id = f"{file_id}_block_{block_index}_{str(uuid.uuid4())[:8]}" # ID de bloque único

            # Registrar asignación tentativa. Se confirmará cuando el DN primario informe o el cliente finalice.
            # Por ahora, el cliente necesita saber a dónde escribir.
            # Las ubicaciones se actualizarán formalmente en block_locations mediante heartbeats o finalize_put.
            
            # Preparamos la respuesta para el cliente
            primary_datanode_info = {
                "id": primary_dn_id,
                "address": self.active_datanodes[primary_dn_id]["address"]
            }
            follower_datanodes_info = [
                {"id": f_id, "address": self.active_datanodes[f_id]["address"]} for f_id in follower_dn_ids
            ]
            
            # Guardar metadatos del bloque en el archivo (aún no tiene ubicaciones confirmadas)
            # Esto se hace realmente en finalize_put_file cuando el cliente confirma escrituras.
            # Aquí solo damos la info para que el cliente escriba.

            response_data = {
                "block_id": block_id,
                "primary_datanode": primary_datanode_info,
                "follower_datanodes": follower_datanodes_info
            }
            return response_data, f"Ubicaciones asignadas para el bloque {block_index} del archivo {file_id}."

    def finalize_put_file(self, file_id: str, success: bool, client_reported_blocks_metadata: list):
        """Finaliza la escritura de un archivo, actualizando metadatos."""
        with self.metadata_lock:
            # Encontrar la entrada del archivo en fs_tree para actualizar su estado
            file_entry_in_tree = None
            parent_of_file = None
            filename_in_tree = None

            def find_file_node_recursive(current_node_children):
                nonlocal file_entry_in_tree, parent_of_file, filename_in_tree
                for name, item in current_node_children.items():
                    if item.get("type") == "file" and item.get("file_id") == file_id:
                        file_entry_in_tree = item
                        # El padre es implícito por el nivel de recursión, necesitamos el dict 'children' del padre.
                        # Esto es más fácil si iteramos desde la raíz.
                        return True 
                    if item.get("type") == "dir":
                        if find_file_node_recursive(item["children"]):
                           return True
                return False
            
            # Mejor encontrarlo iterativamente
            path_to_file_list = [] # Para reconstruir la ruta si es necesario
            q = [ (self.fs_tree["/"]["children"], []) ] # (dict_children_del_nodo_actual, path_parts_hasta_nodo_actual)
            
            found = False
            while q and not found:
                current_children_dict, current_path_parts = q.pop(0)
                for name, item in current_children_dict.items():
                    if item.get("type") == "file" and item.get("file_id") == file_id:
                        file_entry_in_tree = item
                        parent_of_file_children_dict = current_children_dict # El dict 'children' del padre del archivo
                        filename_in_tree = name
                        found = True
                        break
                    if item.get("type") == "dir":
                        q.append( (item["children"], current_path_parts + [name]) )
            
            if not file_entry_in_tree:
                return False, f"File ID {file_id} no encontrado en fs_tree para finalizar."

            if success:
                file_entry_in_tree["status"] = "completed"
                # Actualizar file_to_block_map con la información de bloques que el cliente reportó como escrita.
                # El cliente debe reportar block_id, block_index, size.
                # Las ubicaciones (datanode_ids) se poblarán/confirmarán con los heartbeats.
                processed_blocks_metadata = []
                for client_block_meta in client_reported_blocks_metadata:
                    block_id = client_block_meta["block_id"]
                    processed_blocks_metadata.append({
                        "block_id": block_id,
                        "block_index": client_block_meta["block_index"],
                        "size": client_block_meta["size"],
                        # 'datanode_ids' no se llena aquí, sino con reportes de DNs
                    })
                    # Se asume que el DataNode primario al que escribió el cliente
                    # y sus seguidores eventualmente reportarán tener este bloque.
                    # No actualizamos block_locations directamente aquí basado en el cliente.
                
                self.file_to_block_map[file_id] = sorted(processed_blocks_metadata, key=lambda b: b["block_index"])
                print(f"MetadataManager: Escritura del archivo {file_id} (en {filename_in_tree}) finalizada con éxito. {len(processed_blocks_metadata)} bloques reportados por el cliente.")
                self._save_metadata_to_file()
                return True, f"Archivo {file_id} confirmado y completado."
            else:
                # Si el cliente reporta fallo, limpiamos los metadatos del archivo.
                print(f"MetadataManager: Escritura del archivo {file_id} (en {filename_in_tree}) fallida según el cliente. Limpiando...")
                self._cleanup_file_blocks(file_id) # Limpia file_to_block_map y marca bloques para borrado
                if parent_of_file_children_dict and filename_in_tree in parent_of_file_children_dict:
                    del parent_of_file_children_dict[filename_in_tree] # Elimina de fs_tree
                self._save_metadata_to_file()
                return False, f"Escritura del archivo {file_id} fallida. Metadatos limpiados."


    # --- Operaciones de Lectura de Archivos ---

    def get_file_info_for_get(self, dfs_filepath: str):
        """Obtiene la información de bloques de un archivo para su lectura."""
        with self.metadata_lock:
            norm_path = os.path.normpath(dfs_filepath)
            if not norm_path.startswith("/"):
                return None, "La ruta debe ser absoluta."
            
            file_node = self._get_node_by_path(norm_path)
            if not file_node:
                return None, f"Ruta '{norm_path}' no encontrada."
            if file_node["type"] != "file":
                return None, f"'{norm_path}' no es un archivo."
            if file_node.get("status") != "completed":
                return None, f"El archivo '{norm_path}' no está completo o está en un estado inconsistente ({file_node.get('status')})."

            file_id = file_node["file_id"]
            if file_id not in self.file_to_block_map:
                return None, f"Metadatos de bloques no encontrados para el archivo '{norm_path}' (ID: {file_id})."

            file_blocks_metadata = self.file_to_block_map[file_id]
            blocks_with_locations = []

            for block_meta in file_blocks_metadata:
                block_id = block_meta["block_id"]
                # Obtener ubicaciones de DataNodes activos que tienen este bloque
                active_locations_for_block = []
                if block_id in self.block_locations:
                    for dn_id in self.block_locations[block_id]:
                        if dn_id in self.active_datanodes: # Solo considerar DataNodes activos
                            active_locations_for_block.append({
                                "id": dn_id,
                                "address": self.active_datanodes[dn_id]["address"]
                            })
                
                if not active_locations_for_block:
                    # Si un bloque no tiene ubicaciones activas, el archivo es ilegible.
                    # Podría haber una re-replicación en curso.
                    print(f"MetadataManager WARNING: Bloque {block_id} del archivo {file_id} no tiene ubicaciones activas. El archivo puede ser ilegible.")
                    # Por ahora, devolvemos lo que tenemos; el cliente manejará el error si no puede leer.
                    # O podríamos fallar aquí:
                    # return None, f"Bloque {block_id} del archivo '{norm_path}' no tiene ubicaciones activas. Archivo no disponible."


                blocks_with_locations.append({
                    "block_id": block_id,
                    "block_index": block_meta["block_index"],
                    "size": block_meta["size"],
                    "locations": active_locations_for_block # Lista de DataNodes (id, address)
                })
            
            response_data = {
                "file_id": file_id,
                "filepath_dfs": norm_path, # Añadir la ruta para conveniencia del cliente
                "block_size_config": self.block_size, # Tamaño de bloque de configuración (no necesariamente el tamaño de cada bloque individual)
                "total_size": file_node.get("size",0), # Tamaño total del archivo
                "blocks": blocks_with_locations
            }
            return response_data, f"Información de bloques para '{norm_path}' obtenida."


    # --- Gestión de DataNodes y Heartbeats (gRPC) ---

    def process_datanode_heartbeat(self, datanode_id: str, datanode_address: str, reported_block_ids: list, available_storage_bytes: int):
        """Procesa un heartbeat de un DataNode."""
        with self.metadata_lock:
            current_ts = time.time()
            
            # Actualizar o añadir el DataNode a la lista de activos
            self.active_datanodes[datanode_id] = {
                "address": datanode_address,
                "last_heartbeat_ts": current_ts,
                "blocks_reported": set(reported_block_ids), # Los bloques que este DN dice tener
                "available_storage_bytes": available_storage_bytes
            }
            # print(f"MetadataManager: Heartbeat recibido de DataNode {datanode_id} ({datanode_address}). Bloques: {len(reported_block_ids)}, Espacio Libre: {available_storage_bytes // (1024*1024)}MB")

            # Actualizar la estructura 'block_locations'
            # Primero, registrar los bloques que este DataNode reporta tener.
            for block_id in reported_block_ids:
                if block_id not in self.block_locations:
                    self.block_locations[block_id] = set()
                self.block_locations[block_id].add(datanode_id)

            # Segundo, verificar si este DataNode ya no reporta bloques que antes tenía (y no fueron borrados por el NameNode).
            # Esto es más complejo y podría indicar inconsistencias o eliminaciones no coordinadas.
            # Por ahora, confiamos en que el reporte del DN es la verdad actual para ese DN.
            # Si un bloque desaparece de un reporte y no fue por una orden de borrado,
            # perform_periodic_checks se encargará de la sub-replicación.

            # Preparar comandos para enviar de vuelta al DataNode
            commands_for_datanode = []
            # 1. Comandos de eliminación de bloques
            if datanode_id in self.pending_deletion_blocks_on_datanodes:
                for block_to_delete in list(self.pending_deletion_blocks_on_datanodes[datanode_id]): # Iterar sobre copia
                    commands_for_datanode.append(f"DELETE_BLOCK:{block_to_delete}")
                    self.pending_deletion_blocks_on_datanodes[datanode_id].remove(block_to_delete)
                    # También eliminar este DN de las ubicaciones del bloque si aún estaba allí
                    if block_to_delete in self.block_locations and datanode_id in self.block_locations[block_to_delete]:
                        self.block_locations[block_to_delete].remove(datanode_id)
                        if not self.block_locations[block_to_delete]: # Si el bloque ya no tiene ubicaciones
                             del self.block_locations[block_to_delete]
                if not self.pending_deletion_blocks_on_datanodes[datanode_id]:
                    del self.pending_deletion_blocks_on_datanodes[datanode_id]
            
            # 2. Comandos de replicación (que este DataNode debe iniciar)
            # Esto se manejará en perform_periodic_checks, que decidirá las tareas de replicación.
            # Si este DataNode es elegido como FUENTE para una replicación, se le podría enviar un comando.
            # Ejemplo: commands_for_datanode.append(f"REPLICATE_BLOCK:{block_id_to_replicate}:TO_DATANODE_ADDR:{target_dn_address}")
            
            # Guardar metadatos si hubo cambios significativos por el heartbeat (ej. nuevos bloques reportados)
            # Podría ser muy frecuente, considerar guardado diferido o solo en perform_periodic_checks.
            # self._save_metadata_to_file() 

            return commands_for_datanode


    # --- Mantenimiento y Salud del Sistema ---
    def perform_periodic_checks(self):
        """Realiza chequeos periódicos: DataNodes inactivos, replicación de bloques, etc."""
        with self.metadata_lock:
            current_ts = time.time()
            datanodes_to_remove = []

            # 1. Identificar DataNodes inactivos/muertos
            for dn_id, dn_info in self.active_datanodes.items():
                if current_ts - dn_info["last_heartbeat_ts"] > self.datanode_timeout_seconds:
                    datanodes_to_remove.append(dn_id)
            
            for dn_id in datanodes_to_remove:
                print(f"MetadataManager: DataNode {dn_id} (Timeout). Eliminando de activos.")
                del self.active_datanodes[dn_id]
                # Los bloques que este DataNode tenía ahora están potencialmente sub-replicados.
                # No eliminamos directamente de block_locations aquí, la siguiente fase lo manejará.

            # 2. Verificar y gestionar la replicación de bloques
            all_blocks_in_system = set(self.block_locations.keys()) # Todos los bloques que el NameNode conoce

            for block_id in all_blocks_in_system:
                current_replicas_dns = set()
                if block_id in self.block_locations:
                     # Filtrar solo DataNodes activos
                    for dn_id_loc in list(self.block_locations[block_id]): # Iterar sobre copia
                        if dn_id_loc in self.active_datanodes:
                            current_replicas_dns.add(dn_id_loc)
                        else:
                            # Este DataNode ya no está activo, removerlo de las ubicaciones de este bloque.
                            self.block_locations[block_id].remove(dn_id_loc)
                
                num_active_replicas = len(current_replicas_dns)

                # Verificar si el bloque pertenece a un archivo existente y completado
                # (Si no, podría ser un bloque huérfano o de una subida fallida que debería limpiarse)
                is_block_part_of_active_file = False
                for file_id, blocks_list in self.file_to_block_map.items():
                    # Chequear si el archivo está 'completed'
                    # (Necesitaríamos una forma de obtener el nodo del archivo desde fs_tree para ver su estado)
                    # Por ahora, asumimos que si está en file_to_block_map, es relevante.
                    if any(b_meta["block_id"] == block_id for b_meta in blocks_list):
                        is_block_part_of_active_file = True
                        break
                
                if not is_block_part_of_active_file and block_id in self.block_locations:
                    # Bloque huérfano, marcar para eliminación en los DataNodes que lo tengan
                    print(f"MetadataManager: Bloque huérfano detectado: {block_id}. Marcando para eliminación.")
                    for dn_id_with_orphan in list(self.block_locations.get(block_id, set())):
                        if dn_id_with_orphan not in self.pending_deletion_blocks_on_datanodes:
                            self.pending_deletion_blocks_on_datanodes[dn_id_with_orphan] = set()
                        self.pending_deletion_blocks_on_datanodes[dn_id_with_orphan].add(block_id)
                    del self.block_locations[block_id] # Eliminar de las ubicaciones conocidas
                    continue # Pasar al siguiente bloque

                # Lógica de replicación
                if 0 < num_active_replicas < self.default_replication_factor:
                    # Bloque sub-replicado. Necesita más réplicas.
                    replicas_needed = self.default_replication_factor - num_active_replicas
                    print(f"MetadataManager: Bloque {block_id} está SUB-REPLICADO (tiene {num_active_replicas}, necesita {self.default_replication_factor}). Intentando re-replicar.")
                    
                    # Encontrar DataNodes candidatos para alojar las nuevas réplicas
                    # (activos y que no tengan ya una copia)
                    candidate_target_dns = [
                        dn_id for dn_id in self.active_datanodes 
                        if dn_id not in current_replicas_dns
                    ]
                    random.shuffle(candidate_target_dns) # Para distribuir

                    # Elegir un DataNode fuente (que tenga el bloque y esté activo)
                    if not current_replicas_dns: # No hay fuentes, ¡pérdida de datos para este bloque!
                        print(f"MetadataManager CRITICAL: ¡PÉRDIDA DE DATOS! Bloque {block_id} no tiene réplicas activas.")
                        # Marcar el archivo como corrupto o no disponible.
                        # Eliminar el bloque de self.block_locations si ya no tiene ubicaciones válidas.
                        if block_id in self.block_locations and not self.block_locations[block_id]:
                            del self.block_locations[block_id]
                        continue 

                    source_dn_id = random.choice(list(current_replicas_dns))
                    source_dn_address = self.active_datanodes[source_dn_id]["address"]

                    for _ in range(replicas_needed):
                        if not candidate_target_dns:
                            print(f"MetadataManager WARNING: No hay DataNodes destino disponibles para replicar el bloque {block_id}.")
                            break
                        
                        target_dn_id = candidate_target_dns.pop()
                        target_dn_address = self.active_datanodes[target_dn_id]["address"]
                        
                        # Generar un comando de replicación. El NameNode debe enviar este comando al DataNode FUENTE.
                        # Este comando se puede añadir a una lista que se envíe en la respuesta del heartbeat del DataNode fuente.
                        # O el NameNode podría tener un mecanismo para enviar comandos gRPC directamente a los DataNodes.
                        # Para el diseño actual, lo añadimos a una lista de comandos pendientes para el DataNode fuente.
                        # Esto es conceptual y necesitaría un mecanismo de entrega de comandos más robusto.
                        print(f"MetadataManager: Planificando replicación del bloque {block_id} desde {source_dn_id} ({source_dn_address}) HACIA {target_dn_id} ({target_dn_address})")
                        # Aquí se debería registrar esta tarea para que el NameNode la gestione y envíe el comando.
                        # Por ahora, solo lo imprimimos. En un sistema real, esto iría a una cola de tareas.
                        # El DataNode fuente, al recibir el comando, se conectaría al DataNode destino para enviar el bloque.
                        # Tras la replicación exitosa, el DataNode destino reportaría tener el bloque en su próximo heartbeat.
                        # Y el NameNode actualizaría self.block_locations.

                elif num_active_replicas > self.default_replication_factor:
                    # Bloque sobre-replicado. Eliminar réplicas extra.
                    replicas_to_remove_count = num_active_replicas - self.default_replication_factor
                    print(f"MetadataManager: Bloque {block_id} está SOBRE-REPLICADO (tiene {num_active_replicas}, necesita {self.default_replication_factor}). Eliminando excedentes.")
                    
                    # Elegir DataNodes de los cuales eliminar la réplica (ej. aleatorio, o el más nuevo, o el más cargado)
                    dns_with_this_block = list(current_replicas_dns)
                    random.shuffle(dns_with_this_block)
                    
                    for _ in range(replicas_to_remove_count):
                        if not dns_with_this_block: break
                        dn_to_remove_replica_from = dns_with_this_block.pop()
                        
                        if dn_to_remove_replica_from not in self.pending_deletion_blocks_on_datanodes:
                            self.pending_deletion_blocks_on_datanodes[dn_to_remove_replica_from] = set()
                        self.pending_deletion_blocks_on_datanodes[dn_to_remove_replica_from].add(block_id)
                        print(f"MetadataManager: Marcado bloque {block_id} para eliminación de réplica excedente en DataNode {dn_to_remove_replica_from}.")
                        # La ubicación se eliminará de self.block_locations cuando el DN confirme la eliminación (o deje de reportarlo).

                elif num_active_replicas == 0 and block_id in self.block_locations: # Estaba en block_locations pero ya no tiene DNs activos
                    if is_block_part_of_active_file:
                        print(f"MetadataManager CRITICAL: ¡PÉRDIDA DE DATOS! Bloque {block_id} de un archivo activo no tiene réplicas activas.")
                    # Eliminar la entrada del bloque si ya no tiene ubicaciones.
                    del self.block_locations[block_id]


            # 3. Guardar metadatos si hubo cambios por los chequeos
            self._save_metadata_to_file()
            print("MetadataManager: Chequeos periódicos completados.")