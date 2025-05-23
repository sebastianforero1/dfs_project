# block_store.py en el directorio datanode

import os
import shutil # Para obtener el espacio en disco
import hashlib # Para nombres de archivo seguros si se desea
import threading # Para proteger el acceso a la lista de bloques en memoria

class BlockStore:
    def __init__(self, data_dir_base_path: str, datanode_id: str):
        """
        Inicializa el BlockStore para un DataNode específico.

        Args:
            data_dir_base_path (str): La ruta base donde se crearán los directorios de datos.
            datanode_id (str): El ID único del DataNode al que pertenece este BlockStore.
        """
        self.datanode_id = datanode_id
        # Crear un directorio de datos específico para esta instancia de DataNode
        # Ejemplo: ./datanode_data_storage/dn1-aws-54.242.251.223
        self.data_dir_for_this_dn = os.path.join(data_dir_base_path, self.datanode_id)
        
        # Lock para proteger el acceso a self.in_memory_block_ids si se modifica desde múltiples hilos
        # (ej. escritura de bloque y escaneo de directorio concurrente, aunque el diseño actual es más secuencial por DN)
        self._block_list_lock = threading.Lock()
        self.in_memory_block_ids = set() # Mantiene un registro en memoria de los block_ids almacenados

        self._initialize_storage()

    def _initialize_storage(self):
        """
        Asegura que el directorio de almacenamiento exista y carga los block_ids existentes.
        """
        try:
            if not os.path.exists(self.data_dir_for_this_dn):
                os.makedirs(self.data_dir_for_this_dn)
                print(f"BlockStore [{self.datanode_id}]: Directorio de almacenamiento creado en: {self.data_dir_for_this_dn}")
            else:
                print(f"BlockStore [{self.datanode_id}]: Usando directorio de almacenamiento existente: {self.data_dir_for_this_dn}")
            
            # Cargar/Reconstruir el índice de bloques en memoria al iniciar
            self._load_existing_block_ids_from_disk()

        except OSError as e:
            print(f"BlockStore [{self.datanode_id}] ERROR CRÍTICO: No se pudo crear o acceder al directorio de almacenamiento {self.data_dir_for_this_dn}: {e}")
            # En un sistema real, esto podría ser una falla fatal para el DataNode.
            raise # Re-lanzar la excepción para que el DataNode principal maneje el fallo.

    def _load_existing_block_ids_from_disk(self):
        """
        Escanea el directorio de datos y carga los IDs de los bloques existentes en la lista en memoria.
        Esto es crucial para que el DataNode sepa qué bloques tiene al reiniciar.
        """
        with self._block_list_lock:
            self.in_memory_block_ids.clear()
            loaded_count = 0
            for filename in os.listdir(self.data_dir_for_this_dn):
                # Asumimos que el nombre del archivo es el block_id (o se puede derivar).
                # Si se usaron hashes para los nombres de archivo, se necesitaría un mapeo inverso
                # o almacenar los block_ids originales de otra manera.
                # Para este proyecto, asumimos que el nombre del archivo ES el block_id.
                # TODO: Considerar si los block_ids pueden tener caracteres problemáticos para nombres de archivo.
                #       Usar una función de "escape" o hash podría ser más robusto si es el caso.
                #       Por ahora, el nombre de archivo es el block_id directamente.
                
                # Verificar si es un archivo (no un subdirectorio, si los hubiera)
                file_path = os.path.join(self.data_dir_for_this_dn, filename)
                if os.path.isfile(file_path):
                    # El nombre del archivo es el block_id
                    self.in_memory_block_ids.add(filename)
                    loaded_count += 1
            print(f"BlockStore [{self.datanode_id}]: Cargados {loaded_count} block_ids existentes desde el disco.")


    def _get_block_filepath(self, block_id: str) -> str:
        """
        Construye la ruta completa al archivo de un bloque.
        
        Args:
            block_id (str): El ID del bloque.

        Returns:
            str: La ruta completa al archivo del bloque.
        
        NOTA: Es importante asegurarse de que block_id no contenga caracteres que puedan
        causar problemas en nombres de archivo o permitir path traversal (ej. '../').
        Para un sistema de producción, se debería sanitizar o usar un hash del block_id
        como nombre de archivo. Para este proyecto, asumimos que los block_ids son "seguros".
        """
        # Sanitización básica (ejemplo, no exhaustivo para producción)
        if ".." in block_id or "/" in block_id or "\\" in block_id:
            # Esto podría ser un intento de path traversal o un block_id malformado.
            # En lugar de lanzar un error aquí, que podría ser común, se podría usar un hash.
            # Por ahora, lo permitimos pero con advertencia para el desarrollador.
            print(f"BlockStore [{self.datanode_id}] ADVERTENCIA: Block ID '{block_id}' contiene caracteres potencialmente problemáticos.")
            # Alternativa más segura: usar un hash del block_id como nombre de archivo.
            # safe_filename = hashlib.sha256(block_id.encode()).hexdigest() + ".blk"
            # return os.path.join(self.data_dir_for_this_dn, safe_filename)
        
        # Usar el block_id directamente como nombre de archivo (simple para este proyecto)
        return os.path.join(self.data_dir_for_this_dn, block_id)

    def write_block(self, block_id: str, data: bytes) -> bool:
        """
        Escribe los datos de un bloque en el almacenamiento local.

        Args:
            block_id (str): El ID del bloque a escribir.
            data (bytes): El contenido binario del bloque.

        Returns:
            bool: True si la escritura fue exitosa, False en caso contrario.
        """
        filepath = self._get_block_filepath(block_id)
        try:
            # Escritura atómica (escribir a un temporal y luego renombrar) es más robusta
            # contra fallos durante la escritura, pero más compleja.
            # Para este proyecto, una escritura directa es aceptable.
            with open(filepath, 'wb') as f:
                f.write(data)
            
            # Actualizar el registro en memoria
            with self._block_list_lock:
                self.in_memory_block_ids.add(block_id)
            
            print(f"BlockStore [{self.datanode_id}]: Bloque '{block_id}' (tamaño: {len(data)} bytes) escrito en '{filepath}'.")
            return True
        except IOError as e:
            print(f"BlockStore [{self.datanode_id}] ERROR: No se pudo escribir el bloque '{block_id}' en '{filepath}': {e}")
            # Si la escritura falla, asegurarse de que no quede un archivo parcial corrupto (si es posible)
            # y que no esté en la lista en memoria si no se escribió.
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except OSError:
                    pass # Ignorar error al intentar borrar archivo parcial
            with self._block_list_lock:
                if block_id in self.in_memory_block_ids:
                    self.in_memory_block_ids.remove(block_id)
            return False

    def read_block(self, block_id: str) -> bytes | None:
        """
        Lee los datos de un bloque desde el almacenamiento local.

        Args:
            block_id (str): El ID del bloque a leer.

        Returns:
            bytes | None: El contenido binario del bloque si se encuentra y se lee exitosamente,
                          o None si el bloque no existe o hay un error de lectura.
        """
        filepath = self._get_block_filepath(block_id)
        if not os.path.exists(filepath) or not os.path.isfile(filepath):
            # print(f"BlockStore [{self.datanode_id}]: Bloque '{block_id}' no encontrado en '{filepath}' para lectura.")
            return None
        
        try:
            with open(filepath, 'rb') as f:
                data = f.read()
            # print(f"BlockStore [{self.datanode_id}]: Bloque '{block_id}' (tamaño: {len(data)} bytes) leído de '{filepath}'.")
            return data
        except IOError as e:
            print(f"BlockStore [{self.datanode_id}] ERROR: No se pudo leer el bloque '{block_id}' de '{filepath}': {e}")
            return None

    def delete_block(self, block_id: str) -> bool:
        """
        Elimina un bloque del almacenamiento local.

        Args:
            block_id (str): El ID del bloque a eliminar.

        Returns:
            bool: True si el bloque fue eliminado exitosamente o si no existía.
                  False si ocurrió un error al intentar eliminar un bloque existente.
        """
        filepath = self._get_block_filepath(block_id)
        try:
            if os.path.exists(filepath) and os.path.isfile(filepath):
                os.remove(filepath)
                # Actualizar el registro en memoria
                with self._block_list_lock:
                    if block_id in self.in_memory_block_ids: # Solo remover si estaba
                        self.in_memory_block_ids.remove(block_id)
                print(f"BlockStore [{self.datanode_id}]: Bloque '{block_id}' eliminado de '{filepath}'.")
                return True
            else:
                # Si el bloque no existe, consideramos la operación "exitosa" en el sentido
                # de que el estado deseado (bloque no presente) se cumple.
                # print(f"BlockStore [{self.datanode_id}]: Bloque '{block_id}' no encontrado para eliminación, se considera eliminado.")
                # Asegurarse de que no esté en la lista en memoria si no está en disco
                with self._block_list_lock:
                    if block_id in self.in_memory_block_ids:
                        self.in_memory_block_ids.remove(block_id)
                return True
        except OSError as e:
            print(f"BlockStore [{self.datanode_id}] ERROR: No se pudo eliminar el bloque '{block_id}' de '{filepath}': {e}")
            return False

    def get_stored_block_ids(self) -> list[str]:
        """
        Devuelve una lista de los IDs de todos los bloques actualmente almacenados.
        Esta información se utiliza para los reportes de bloques en los heartbeats.
        """
        with self._block_list_lock:
            # Devolver una copia para evitar modificaciones concurrentes de la lista devuelta
            return list(self.in_memory_block_ids)

    def get_available_storage_bytes(self) -> int:
        """
        Calcula y devuelve el espacio de almacenamiento libre disponible en el volumen
        donde se encuentra el directorio de datos de este DataNode.

        Returns:
            int: Espacio libre en bytes. Devuelve 0 si no se puede determinar.
        """
        try:
            # shutil.disk_usage devuelve un named tuple con total, used, y free space.
            # Necesitamos el espacio libre en el path donde se guardan los datos.
            # Si data_dir_for_this_dn no existe aún (ej. al primer inicio antes de crear dirs),
            # usar su padre o un path que se sepa que está en el mismo volumen.
            path_to_check = self.data_dir_for_this_dn
            if not os.path.exists(path_to_check):
                # Si el dir específico del DN no existe, chequear el dir base.
                # Esto puede pasar si el DN está arrancando por primera vez y aún no ha creado su dir.
                path_to_check = os.path.dirname(self.data_dir_for_this_dn) 
                if not os.path.exists(path_to_check): # Si ni el padre existe, usar el dir actual como fallback
                    path_to_check = "."

            total, used, free = shutil.disk_usage(path_to_check)
            # print(f"BlockStore [{self.datanode_id}]: Espacio disponible en '{path_to_check}': {free // (1024*1024)} MB")
            return free
        except Exception as e:
            print(f"BlockStore [{self.datanode_id}] ERROR: No se pudo obtener el espacio disponible en disco para '{self.data_dir_for_this_dn}': {e}")
            return 0 # Devolver 0 en caso de error

    def ensure_data_directory_exists(self):
        """
        Método de conveniencia para ser llamado explícitamente si la creación del directorio
        no se garantiza completamente en __init__ (aunque ya lo hace).
        """
        if not os.path.exists(self.data_dir_for_this_dn):
            try:
                os.makedirs(self.data_dir_for_this_dn)
                print(f"BlockStore [{self.datanode_id}]: Directorio de almacenamiento verificado/creado en: {self.data_dir_for_this_dn}")
            except OSError as e:
                print(f"BlockStore [{self.datanode_id}] ERROR CRÍTICO: No se pudo crear el directorio de almacenamiento {self.data_dir_for_this_dn} en ensure_data_directory_exists: {e}")
                raise