# cli.py en el directorio client

import argparse
import os
import sys

from .client_dfs_sdk import DFSClientSDK
from . import config_client as cfg_client # Importar para obtener el valor por defecto para el help

def main_cli():
    parser = argparse.ArgumentParser(
        description="Cliente CLI para interactuar con el Sistema de Archivos Distribuido (DFS).",
        prog="dfs-client"
    )
    parser.add_argument(
        "--namenode-url",
        type=str,
        default=None, # El SDK manejará el default usando cfg_client si este es None
        help=(
            f"URL base de la API REST del NameNode (ej: http://<ip>:<port>). "
            f"Si no se especifica, se usa el valor de la variable de entorno NAMENODE_REST_URL "
            f"o el predeterminado en la configuración del SDK (que podría ser: {cfg_client.NAMENODE_REST_URL})."
        )
    )
    # Añadir argumentos para los timeouts si se desea permitir sobrescribirlos desde la CLI
    parser.add_argument(
        "--http-timeout",
        type=int,
        default=None, # El SDK usará su default de cfg_client
        help=f"Timeout en segundos para las solicitudes HTTP al NameNode (defecto SDK: {cfg_client.DEFAULT_HTTP_TIMEOUT_SECONDS}s)."
    )
    parser.add_argument(
        "--grpc-timeout",
        type=int,
        default=None, # El SDK usará su default de cfg_client
        help=f"Timeout en segundos para operaciones gRPC con DataNodes (defecto SDK: {cfg_client.DEFAULT_GRPC_BLOCK_OP_TIMEOUT_SECONDS}s)."
    )


    subparsers = parser.add_subparsers(dest="command", title="Comandos Disponibles", required=True)

    # --- Comandos (ls, mkdir, rmdir, put, get, rm, cd, pwd) ---
    # (La definición de los subparsers es idéntica a la proporcionada anteriormente)
    # Solo se muestra ls como ejemplo, el resto no cambia su definición de argparse.
    parser_ls = subparsers.add_parser("ls", help="Listar el contenido de un directorio en el DFS.")
    parser_ls.add_argument("dfs_path", type=str, nargs='?', default=".", help="Ruta del directorio en el DFS a listar (def: directorio actual).")

    parser_mkdir = subparsers.add_parser("mkdir", help="Crear un nuevo directorio en el DFS.")
    parser_mkdir.add_argument("dfs_path", type=str, help="Ruta DFS para el nuevo directorio.")

    parser_rmdir = subparsers.add_parser("rmdir", help="Eliminar un directorio vacío del DFS.")
    parser_rmdir.add_argument("dfs_path", type=str, help="Ruta DFS del directorio a eliminar.")

    parser_put = subparsers.add_parser("put", help="Subir un archivo local al DFS.")
    parser_put.add_argument("local_filepath", type=str, help="Ruta del archivo local.")
    parser_put.add_argument("dfs_filepath", type=str, help="Ruta DFS de destino.")

    parser_get = subparsers.add_parser("get", help="Descargar un archivo del DFS.")
    parser_get.add_argument("dfs_filepath", type=str, help="Ruta DFS del archivo a descargar.")
    parser_get.add_argument("local_save_path", type=str, help="Ruta local para guardar el archivo.")

    parser_rm = subparsers.add_parser("rm", help="Eliminar un archivo del DFS.")
    parser_rm.add_argument("dfs_filepath", type=str, help="Ruta DFS del archivo a eliminar.")

    parser_cd = subparsers.add_parser("cd", help="Cambiar el directorio DFS actual del SDK.")
    parser_cd.add_argument("dfs_path", type=str, help="Ruta DFS a la cual cambiar.")

    parser_pwd = subparsers.add_parser("pwd", help="Mostrar el directorio DFS actual del SDK.")


    try:
        args = parser.parse_args()
    except SystemExit as e:
        if e.code == 2 and not any(arg in sys.argv for arg in ['-h', '--help']): pass
        return

    # Inicializar el SDK del cliente DFS, pasando los argumentos de la CLI si se proporcionaron
    sdk = DFSClientSDK(
        namenode_rest_url=args.namenode_url, # Será None si no se dio, el SDK usará cfg_client
        http_timeout=args.http_timeout,     # Será None si no se dio, el SDK usará cfg_client
        grpc_timeout=args.grpc_timeout      # Será None si no se dio, el SDK usará cfg_client
    )

    # Ejecutar el comando correspondiente
    # (La lógica if/elif/else para args.command es idéntica a la proporcionada anteriormente)
    try:
        if args.command == "ls": sdk.ls(args.dfs_path)
        elif args.command == "mkdir": sdk.mkdir(args.dfs_path)
        elif args.command == "rmdir": sdk.rmdir(args.dfs_path)
        elif args.command == "put": sdk.put(args.local_filepath, args.dfs_filepath)
        elif args.command == "get": sdk.get(args.dfs_filepath, args.local_save_path)
        elif args.command == "rm": sdk.rm(args.dfs_filepath)
        elif args.command == "cd":
            if sdk.cd(args.dfs_path): print(f"Directorio DFS actual (SDK) cambiado a: {sdk.pwd()}")
            else: print(f"No se pudo cambiar el directorio a: {args.dfs_path}")
        elif args.command == "pwd": print(sdk.pwd())
        else: parser.print_help(); sys.exit(1)
    except Exception as e:
        print(f"CLI ERROR: Ocurrió una excepción no controlada ejecutando '{args.command}': {e}")
        sys.exit(1)

if __name__ == '__main__':
    main_cli()