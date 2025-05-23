# api_endpoints.py en el directorio namenode

from fastapi import FastAPI, HTTPException, Body, Query, status
from pydantic import BaseModel, Field # Field para validaciones y ejemplos
from typing import List, Dict, Any, Optional

# Importar la instancia de MetadataManager (asumiendo que se inyectará o será accesible)
# Esta es una referencia que se establecerá en main_namenode.py
metadata_manager = None # Se asignará desde main_namenode.py

# --- Modelos Pydantic para las solicitudes y respuestas de la API ---

class PathRequest(BaseModel):
    path: str = Field(..., example="/usr/data/filename.txt", description="Ruta DFS absoluta.")

class MkdirResponse(BaseModel):
    success: bool
    message: str
    path: Optional[str] = None

class LsItem(BaseModel):
    name: str
    type: str = Field(..., example="file|dir", description="Tipo de entrada: 'file' o 'dir'.")
    size: Optional[int] = Field(None, example=102400, description="Tamaño del archivo en bytes (solo para tipo 'file').")
    file_id: Optional[str] = Field(None, example="uuid-...", description="ID único del archivo (solo para tipo 'file').")

class LsResponse(BaseModel):
    success: bool
    message: str
    path: str
    listing: List[LsItem] = []

class RmResponse(BaseModel):
    success: bool
    message: str
    path: Optional[str] = None

class FilePutInitiateRequest(BaseModel):
    filepath: str = Field(..., example="/shared/archive.zip", description="Ruta DFS completa donde se escribirá el archivo.")
    filesize: int = Field(..., gt=0, example=204857600, description="Tamaño total del archivo en bytes.") # gt=0 para tamaño > 0; archivos vacíos se manejarán

class FilePutInitiateResponse(BaseModel):
    success: bool
    message: str
    file_id: Optional[str] = None
    block_size: Optional[int] = None # Tamaño de bloque que el cliente debe usar
    num_blocks: Optional[int] = None

class DataNodeLocationInfo(BaseModel):
    id: str = Field(..., example="dn1_aws", description="ID del DataNode.")
    address: str = Field(..., example="54.242.251.223:50061", description="Dirección (IP:puerto) del DataNode para gRPC.")

class BlockAllocationResponse(BaseModel):
    success: bool
    message: str
    block_id: Optional[str] = None
    primary_datanode: Optional[DataNodeLocationInfo] = None
    follower_datanodes: List[DataNodeLocationInfo] = []

class ClientBlockMetadata(BaseModel):
    block_id: str = Field(..., example="uuid-file_block_0_uuid-block")
    block_index: int = Field(..., ge=0, example=0) # ge=0 para índice >= 0
    size: int = Field(..., ge=0, example=67108864) # ge=0 para tamaño >= 0

class FilePutFinalizeRequest(BaseModel):
    file_id: str = Field(..., example="uuid-...")
    success: bool # Si el cliente considera que la subida de todos los bloques fue exitosa
    block_metadata: List[ClientBlockMetadata] = Field(
        default_factory=list,
        description="Metadatos de cada bloque que el cliente escribió exitosamente."
    )

class FilePutFinalizeResponse(BaseModel):
    success: bool
    message: str

class BlockLocationForGet(BaseModel):
    block_id: str
    block_index: int
    size: int # Tamaño real de este bloque
    locations: List[DataNodeLocationInfo] = [] # DataNodes activos que tienen este bloque

class FileGetInfoResponse(BaseModel):
    success: bool
    message: str
    file_id: Optional[str] = None
    filepath_dfs: Optional[str] = None
    block_size_config: Optional[int] = None # Tamaño de bloque configurado en el NameNode
    total_size: Optional[int] = None # Tamaño total del archivo
    blocks: List[BlockLocationForGet] = []


# --- Aplicación FastAPI ---
# El prefijo /fs se usará para operaciones generales del sistema de archivos,
# y /file para operaciones específicas de manipulación de archivos (put/get).
app_namenode_rest = FastAPI(
    title="NameNode DFS API",
    description="API para gestionar metadatos y coordinar operaciones de archivos en el DFS.",
    version="0.1.0"
)

# --- Endpoints del Sistema de Archivos (Metadatos) ---

@app_namenode_rest.post("/fs/mkdir",
                        response_model=MkdirResponse,
                        status_code=status.HTTP_201_CREATED,
                        summary="Crear un nuevo directorio.",
                        tags=["FileSystem Metadata"])
async def api_mkdir(req: PathRequest = Body(...)):
    """
    Crea un nuevo directorio en la ruta especificada en el DFS.
    La ruta debe ser absoluta.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")
    
    success, message = metadata_manager.mkdir(req.path)
    if not success:
        # Determinar el código de error apropiado
        if "ya existe" in message:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=message)
        elif "ruta padre no existe" in message or "ruta debe ser absoluta" in message:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)
    return MkdirResponse(success=True, message=message, path=req.path)

@app_namenode_rest.get("/fs/ls",
                       response_model=LsResponse,
                       summary="Listar contenido de un directorio.",
                       tags=["FileSystem Metadata"])
async def api_ls(path: str = Query(..., example="/usr/data", description="Ruta DFS absoluta del directorio a listar.")):
    """
    Lista el contenido (archivos y subdirectorios) de un directorio específico en el DFS.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")

    listing, message = metadata_manager.ls(path)
    if listing is None: # Indica error, por ejemplo, path no encontrado o no es directorio
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)
    
    return LsResponse(success=True, message=message, path=path, listing=listing)

@app_namenode_rest.post("/fs/rmdir",
                       response_model=RmResponse,
                       summary="Eliminar un directorio (si está vacío).",
                       tags=["FileSystem Metadata"])
async def api_rmdir(req: PathRequest = Body(...)):
    """
    Elimina un directorio vacío del DFS.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")
    
    success, message = metadata_manager.rmdir(req.path)
    if not success:
        if "no encontrado" in message:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)
        elif "no está vacío" in message:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=message)
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return RmResponse(success=True, message=message, path=req.path)

@app_namenode_rest.delete("/fs/rm", # Usar DELETE para la semántica correcta de borrado
                        response_model=RmResponse,
                        summary="Eliminar un archivo.",
                        tags=["FileSystem Metadata"])
async def api_rm_file(path: str = Query(..., example="/config/old_settings.cfg", description="Ruta DFS absoluta del archivo a eliminar.")):
    """
    Elimina un archivo del DFS. Esto también instruirá a los DataNodes
    para que eliminen los bloques correspondientes.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")
    
    success, message = metadata_manager.rm(path)
    if not success:
        if "no encontrado" in message or "no es un archivo" in message:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)
        else: # Otro tipo de error (ej: metadatos inconsistentes)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)
    return RmResponse(success=True, message=message, path=path)


# --- Endpoints para Operaciones de Archivos (PUT/GET) ---

@app_namenode_rest.post("/file/initiate_put",
                        response_model=FilePutInitiateResponse,
                        summary="Iniciar la subida (put) de un archivo.",
                        tags=["File Operations"])
async def api_initiate_put_file(req: FilePutInitiateRequest = Body(...)):
    """
    Cliente informa al NameNode que desea escribir un nuevo archivo (o reemplazar uno existente).
    El NameNode crea/actualiza metadatos y devuelve el file_id, el tamaño de bloque
    y el número de bloques que el cliente deberá escribir.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")
    
    # Manejar archivos de tamaño 0. filesize debe ser >=0.
    if req.filesize < 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El tamaño del archivo no puede ser negativo.")

    response_data, message = metadata_manager.initiate_put_file(req.filepath, req.filesize)
    if response_data is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message) # Podría ser 409 si hay conflicto no manejado

    return FilePutInitiateResponse(success=True, message=message, **response_data)


@app_namenode_rest.get("/file/allocate_block", # Cambiado de 'block_allocation' para más claridad
                       response_model=BlockAllocationResponse,
                       summary="Solicitar asignación de DataNodes para un bloque.",
                       tags=["File Operations"])
async def api_allocate_block_for_put(
    file_id: str = Query(..., example="uuid-...", description="ID del archivo para el cual se escribe el bloque."),
    block_index: int = Query(..., ge=0, example=0, description="Índice secuencial del bloque dentro del archivo."),
    block_size_actual: int = Query(..., ge=0, example=67108864, description="Tamaño real en bytes de este bloque específico.")
):
    """
    Cliente solicita al NameNode las ubicaciones (DataNode primario y seguidores)
    donde debe escribir un bloque específico de un archivo.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")

    response_data, message = metadata_manager.allocate_block_locations(file_id, block_index, block_size_actual)
    if response_data is None:
        # Podría ser 503 si no hay suficientes DataNodes, o 404 si el file_id no es válido.
        if "No hay suficientes DataNodes" in message:
             raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=message)
        elif "File ID no encontrado" in message:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)
        else:
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)
             
    return BlockAllocationResponse(success=True, message=message, **response_data)


@app_namenode_rest.post("/file/finalize_put",
                        response_model=FilePutFinalizeResponse,
                        summary="Finalizar la subida (put) de un archivo.",
                        tags=["File Operations"])
async def api_finalize_put_file(req: FilePutFinalizeRequest = Body(...)):
    """
    Cliente informa al NameNode si la escritura de todos los bloques del archivo
    fue exitosa o fallida. El NameNode actualiza el estado del archivo.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")

    success, message = metadata_manager.finalize_put_file(
        req.file_id,
        req.success,
        [b.dict() for b in req.block_metadata] # Convertir lista de Pydantic a lista de dicts
    )
    if not success:
        # El error podría ser por varias razones, el mensaje del metadata_manager lo indicará.
        # Si el file_id no se encuentra, sería un 404. Otros podrían ser 400 o 500.
        if "File ID no encontrado" in message:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
            
    return FilePutFinalizeResponse(success=True, message=message)


@app_namenode_rest.get("/file/info_for_get",
                       response_model=FileGetInfoResponse,
                       summary="Obtener información de bloques para descargar (get) un archivo.",
                       tags=["File Operations"])
async def api_get_file_info_for_get(
    filepath: str = Query(..., example="/docs/manual.pdf", description="Ruta DFS absoluta del archivo a leer.")
):
    """
    Cliente solicita al NameNode la información de un archivo para su lectura,
    incluyendo la lista ordenada de sus bloques y las ubicaciones de los DataNodes
    activos que contienen dichos bloques.
    """
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")

    response_data, message = metadata_manager.get_file_info_for_get(filepath)
    if response_data is None:
        if "no encontrado" in message or "no es un archivo" in message:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)
        elif "no está completo" in message or "no tiene ubicaciones activas" in message:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=message) # Conflicto, el archivo no está listo/disponible
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)
            
    return FileGetInfoResponse(success=True, message=message, **response_data)

# --- Endpoints de Estado/Debug (Opcional) ---
class NameNodeStatus(BaseModel):
    active_datanodes_count: int
    total_files: int # Podría ser más complejo de calcular precisamente sin iterar todo fs_tree
    total_blocks_tracked: int
    # más información útil...

@app_namenode_rest.get("/status",
                        response_model=NameNodeStatus,
                        summary="Obtener estado básico del NameNode.",
                        tags=["Status"])
async def api_get_status():
    """Devuelve información básica sobre el estado actual del NameNode."""
    if metadata_manager is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="MetadataManager no inicializado.")
    
    with metadata_manager.metadata_lock: # Acceder a las estructuras de forma segura
        active_dns = len(metadata_manager.active_datanodes)
        # Calcular total_files es un poco más intensivo, simplificado aquí
        files_count = sum(1 for fid in metadata_manager.file_to_block_map) # Cuenta archivos con bloques mapeados
        blocks_count = len(metadata_manager.block_locations)

    return NameNodeStatus(
        active_datanodes_count=active_dns,
        total_files=files_count,
        total_blocks_tracked=blocks_count
    )