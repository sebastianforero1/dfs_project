# replication_manager.py en el directorio namenode

import random

# Este módulo contendrá la lógica para tomar decisiones sobre la replicación de bloques,
# como la selección de DataNodes para nuevas réplicas o la elección de réplicas
# para eliminar en caso de sobre-replicación.

def choose_datanodes_for_new_replicas(
    block_id: str,
    current_replica_datanode_ids: set,
    active_datanodes_info: dict,
    num_new_replicas_needed: int
) -> list:
    """
    Elige DataNodes destino para alojar nuevas réplicas de un bloque.

    Args:
        block_id (str): El ID del bloque que necesita nuevas réplicas.
        current_replica_datanode_ids (set): Un conjunto de IDs de DataNodes que ya tienen una réplica activa de este bloque.
        active_datanodes_info (dict): Un diccionario con información de todos los DataNodes activos.
                                      Formato: {dn_id: {"address": "ip:port", "available_storage_bytes": X, ...}}
        num_new_replicas_needed (int): Cuántas nuevas réplicas se necesitan para este bloque.

    Returns:
        list: Una lista de IDs de DataNodes elegidos como destino para las nuevas réplicas.
              La lista puede ser más corta que num_new_replicas_needed si no hay suficientes
              DataNodes candidatos.
    """
    candidate_target_datanodes = []
    for dn_id, dn_info in active_datanodes_info.items():
        if dn_id not in current_replica_datanode_ids:
            # Criterios para ser un candidato (se pueden expandir):
            # 1. Debe estar activo (implícito por estar en active_datanodes_info).
            # 2. No debe tener ya una copia del bloque.
            # 3. (Opcional avanzado) Debe tener suficiente espacio (dn_info.get("available_storage_bytes", 0) > umbral_espacio).
            # 4. (Opcional avanzado) No estar en el mismo rack/zona de disponibilidad que otras réplicas (no implementado aquí).
            candidate_target_datanodes.append(dn_id)

    if not candidate_target_datanodes:
        print(f"ReplicationManager: No hay DataNodes candidatos para nuevas réplicas del bloque {block_id}.")
        return []

    # Estrategia de selección: aleatoria entre los candidatos.
    # Se podrían implementar estrategias más sofisticadas (ej. menos cargado, más espacio libre).
    random.shuffle(candidate_target_datanodes)

    # Seleccionar hasta el número necesitado de réplicas.
    num_to_select = min(len(candidate_target_datanodes), num_new_replicas_needed)
    chosen_targets = candidate_target_datanodes[:num_to_select]

    if len(chosen_targets) < num_new_replicas_needed:
        print(f"ReplicationManager WARNING: No se pudieron encontrar suficientes ({len(chosen_targets)} de {num_new_replicas_needed}) DataNodes destino para el bloque {block_id}.")
    
    # print(f"ReplicationManager: Para bloque {block_id}, elegidos {len(chosen_targets)} DataNodes destino para nuevas réplicas: {chosen_targets}")
    return chosen_targets


def choose_source_datanode_for_replication(
    block_id: str,
    current_replica_datanode_ids: set,
    active_datanodes_info: dict
) -> str | None:
    """
    Elige un DataNode fuente (que ya tiene una copia activa del bloque) para iniciar la replicación.

    Args:
        block_id (str): El ID del bloque a replicar.
        current_replica_datanode_ids (set): IDs de DataNodes que tienen una réplica activa.
        active_datanodes_info (dict): Información de los DataNodes activos.

    Returns:
        str | None: El ID del DataNode fuente elegido, o None si no se encuentra uno adecuado.
    """
    if not current_replica_datanode_ids:
        print(f"ReplicationManager CRITICAL: No hay DataNodes fuente con una réplica activa del bloque {block_id} para iniciar la replicación.")
        return None

    # Estrategia de selección: aleatoria entre los que tienen la réplica.
    # Podría ser más sofisticado (ej. el menos cargado).
    # Asegurarse de que los DataNodes en current_replica_datanode_ids sigan activos.
    valid_source_candidates = [dn_id for dn_id in current_replica_datanode_ids if dn_id in active_datanodes_info]

    if not valid_source_candidates:
        print(f"ReplicationManager CRITICAL: Las réplicas existentes del bloque {block_id} no están en DataNodes activos.")
        return None
        
    chosen_source = random.choice(valid_source_candidates)
    # print(f"ReplicationManager: Para bloque {block_id}, elegido DataNode fuente {chosen_source} para iniciar replicación.")
    return chosen_source


def choose_replicas_for_deletion(
    block_id: str,
    current_replica_datanode_ids: set,
    active_datanodes_info: dict,
    num_replicas_to_delete: int
) -> list:
    """
    Elige qué réplicas de un bloque se deben eliminar en caso de sobre-replicación.

    Args:
        block_id (str): El ID del bloque sobre-replicado.
        current_replica_datanode_ids (set): IDs de DataNodes que tienen una réplica activa.
        active_datanodes_info (dict): Información de los DataNodes activos.
        num_replicas_to_delete (int): Cuántas réplicas se deben eliminar.

    Returns:
        list: Una lista de IDs de DataNodes de los cuales se debe eliminar una réplica.
    """
    if num_replicas_to_delete <= 0:
        return []

    # Asegurarse de que los DataNodes en current_replica_datanode_ids sigan activos.
    # Aunque para la eliminación, si un DN está inactivo y tiene una réplica "extra",
    # esa réplica ya no contaría para la sobre-replicación activa.
    # La selección debe ser de réplicas *activas* que son excedentes.
    
    candidates_for_deletion = list(current_replica_datanode_ids) # Convertir set a lista para poder barajar/seleccionar

    if not candidates_for_deletion or len(candidates_for_deletion) <= (len(current_replica_datanode_ids) - num_replicas_to_delete) :
        # No hay suficientes réplicas para eliminar o algo es inconsistente.
        # Esto no debería pasar si num_replicas_to_delete es positivo y se calculó correctamente.
        print(f"ReplicationManager WARNING: No se pueden elegir réplicas para eliminar del bloque {block_id}. Candidatos: {len(candidates_for_deletion)}, a eliminar: {num_replicas_to_delete}")
        return []

    # Estrategia de selección: aleatoria.
    # Podrían considerarse otros factores:
    # - No eliminar la réplica "más antigua" o "primaria" si ese concepto existe post-escritura.
    # - Elegir DataNodes con más espacio libre (contraintuitivo, se querría liberar en los más llenos).
    # - Elegir DataNodes que tengan muchos bloques (para balancear).
    random.shuffle(candidates_for_deletion)

    num_to_select = min(len(candidates_for_deletion), num_replicas_to_delete)
    chosen_dns_for_deletion = candidates_for_deletion[:num_to_select]
    
    # print(f"ReplicationManager: Para bloque {block_id}, elegidas {len(chosen_dns_for_deletion)} réplicas para eliminar de DataNodes: {chosen_dns_for_deletion}")
    return chosen_dns_for_deletion

# Podrían añadirse más funciones aquí si se desarrollan políticas de replicación más complejas,
# por ejemplo, basadas en la ubicación geográfica (racks, zonas) de los DataNodes,
# o en su carga de trabajo actual. Para este proyecto, las funciones anteriores
# con selección aleatoria o basada en disponibilidad son un buen punto de partida.