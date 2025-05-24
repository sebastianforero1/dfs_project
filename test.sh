#!/bin/bash

# Script de pruebas para el Cliente DFS


echo "**********************************************"
echo "*** INICIANDO PRUEBAS DEL CLIENTE DFS ***"
echo "**********************************************"
echo "Usando NameNode en: ${NAMENODE_REST_URL}"
echo ""

# Función para ejecutar comandos y mostrar su salida
run_command() {
    echo "EJECUTANDO: $@"
    "$@" # Ejecuta el comando
    echo "----------------------------------------------"
    sleep 1 # Pequeña pausa entre comandos
}

# Crear archivos locales de prueba
echo "--- Creando archivos locales para prueba ---"
BASE_CONTENT="Este es el contenido de prueba para el archivo DFS."
FECHA=$(date +"%Y-%m-%d_%H-%M-%S")

# Archivo vacío
touch archivo_vacio.txt
echo "Creado archivo_vacio.txt"

# Archivo pequeño
echo "${BASE_CONTENT} - Pequeño - ${FECHA}" > archivo_pequeno.txt
echo "Creado archivo_pequeno.txt"

# Archivo mediano (simularemos con más líneas)
# Suponiendo un tamaño de bloque de 1MB y queriendo ~1.5 bloques
# Cada línea tiene ~60-70 bytes. 1MB = 1048576 bytes.
# Para 1.5MB, necesitaríamos ~25000 líneas. Para prueba, haremos algo más manejable.
# Crearemos un archivo de unas pocas KBs que probablemente use un solo bloque o dos pequeños.
echo "${BASE_CONTENT} - Mediano - ${FECHA}" > archivo_mediano.txt
for i in {1..200}; do
  echo "Línea de relleno número $i para archivo_mediano.txt" >> archivo_mediano.txt
done
echo "Creado archivo_mediano.txt con $(wc -c < archivo_mediano.txt) bytes"

# Archivo para probar WORM (reemplazo)
echo "Contenido original para reemplazo - ${FECHA}" > archivo_worm_v1.txt
echo "Creado archivo_worm_v1.txt"
echo "----------------------------------------------"
sleep 1

# Limpieza inicial (opcional, para asegurar un estado limpio en el DFS)
echo "*** Limpieza inicial en DFS (opcional) ***"
run_command python -m client.cli rm /pruebas_dfs/grande.txt # Si existiera de pruebas anteriores
run_command python -m client.cli rm /pruebas_dfs/mediano.txt
run_command python -m client.cli rm /pruebas_dfs/pequeno.txt
run_command python -m client.cli rm /pruebas_dfs/vacio.txt
run_command python -m client.cli rm /pruebas_dfs/worm_test.txt
run_command python -m client.cli rmdir /pruebas_dfs/subdir # Si existiera
run_command python -m client.cli rmdir /pruebas_dfs
echo "----------------------------------------------"

# --- Pruebas de Comandos de Directorio ---
echo "*** Probando Comandos de Directorio ***"
run_command python -m client.cli mkdir /pruebas_dfs
run_command python -m client.cli mkdir /pruebas_dfs/subdir
run_command python -m client.cli ls /
run_command python -m client.cli ls /pruebas_dfs
run_command python -m client.cli pwd
run_command python -m client.cli cd /pruebas_dfs
run_command python -m client.cli pwd
run_command python -m client.cli ls . # Debería listar contenido de /pruebas_dfs
run_command python -m client.cli cd subdir
run_command python -m client.cli pwd
run_command python -m client.cli ls . # Debería listar contenido de /pruebas_dfs/subdir (vacío)
run_command python -m client.cli cd / # Volver a la raíz

# --- Pruebas de Operaciones de Archivo ---
echo "*** Probando Operaciones de Archivo (put, get) ***"
# PUT
run_command python -m client.cli put archivo_vacio.txt /pruebas_dfs/vacio.txt
run_command python -m client.cli put archivo_pequeno.txt /pruebas_dfs/pequeno.txt
run_command python -m client.cli put archivo_mediano.txt /pruebas_dfs/mediano.txt
echo "Archivos subidos. Verificando con ls:"
run_command python -m client.cli ls /pruebas_dfs

# GET y Verificación con diff
echo "Descargando y verificando archivos..."
run_command python -m client.cli get /pruebas_dfs/vacio.txt ./desc_vacio.txt
diff archivo_vacio.txt ./desc_vacio.txt && echo "OK: vacio.txt verificado." || echo "ERROR: vacio.txt difiere."

run_command python -m client.cli get /pruebas_dfs/pequeno.txt ./desc_pequeno.txt
diff archivo_pequeno.txt ./desc_pequeno.txt && echo "OK: pequeno.txt verificado." || echo "ERROR: pequeno.txt difiere."

run_command python -m client.cli get /pruebas_dfs/mediano.txt ./desc_mediano.txt
diff archivo_mediano.txt ./desc_mediano.txt && echo "OK: mediano.txt verificado." || echo "ERROR: mediano.txt difiere."
echo "----------------------------------------------"

# --- Prueba de WORM (Write-Once-Read-Many - reemplazo) ---
echo "*** Probando Característica WORM (reemplazo) ***"
run_command python -m client.cli put archivo_worm_v1.txt /pruebas_dfs/worm_test.txt
echo "Contenido nuevo para reemplazo - ${FECHA}" > archivo_worm_v2.txt
run_command python -m client.cli put archivo_worm_v2.txt /pruebas_dfs/worm_test.txt # Sobrescribir
run_command python -m client.cli get /pruebas_dfs/worm_test.txt ./desc_worm_v2.txt
diff archivo_worm_v2.txt ./desc_worm_v2.txt && echo "OK: WORM (reemplazo) verificado." || echo "ERROR: WORM (reemplazo) falló."
echo "----------------------------------------------"

# --- Prueba de Tolerancia a Fallos (Manual) ---
echo "*** Prueba de Tolerancia a Fallos (requiere intervención manual) ***"
echo "1. Sube un archivo (ej. mediano.txt si no está ya)."
# run_command python -m client.cli put archivo_mediano.txt /pruebas_dfs/mediano_tf.txt
echo "2. Verifica que se puede descargar."
# run_command python -m client.cli get /pruebas_dfs/mediano_tf.txt ./desc_mediano_tf1.txt
# diff archivo_mediano.txt ./desc_mediano_tf1.txt && echo "OK: Descarga inicial para TF." || echo "ERROR: Descarga inicial TF."
echo "3. DETÉN UNO DE TUS DATANODES (ej. DataNode-1) desde su consola EC2 (sudo pkill -f datanode)."
echo "4. Espera ~30-45 segundos para que el NameNode lo marque como inactivo."
read -p "PRESIONA ENTER CUANDO HAYAS DETENIDO UN DATANODE Y ESPERADO..."
echo "5. Intenta descargar el archivo de nuevo. Debería funcionar si la replicación (a DataNode-2) fue exitosa."
run_command python -m client.cli get /pruebas_dfs/mediano.txt ./desc_mediano_tf2.txt # Usamos el mediano.txt original
diff archivo_mediano.txt ./desc_mediano_tf2.txt && echo "OK: Descarga después de fallo de DN." || echo "ERROR: Descarga después de fallo de DN."
echo "RECUERDA REINICIAR EL DATANODE DETENIDO DESPUÉS DE ESTA PRUEBA."
echo "----------------------------------------------"

# --- Pruebas de Eliminación ---
echo "*** Probando Comandos de Eliminación (rm, rmdir) ***"
run_command python -m client.cli ls /pruebas_dfs # Mostrar contenido antes de borrar
run_command python -m client.cli rm /pruebas_dfs/vacio.txt
run_command python -m client.cli rm /pruebas_dfs/pequeno.txt
run_command python -m client.cli rm /pruebas_dfs/mediano.txt
run_command python -m client.cli rm /pruebas_dfs/worm_test.txt
# run_command python -m client.cli rm /pruebas_dfs/mediano_tf.txt # Si hiciste la prueba de TF con este archivo
run_command python -m client.cli ls /pruebas_dfs # Debería estar vacío de archivos
run_command python -m client.cli rmdir /pruebas_dfs/subdir
run_command python -m client.cli rmdir /pruebas_dfs
run_command python -m client.cli ls / # /pruebas_dfs no debería existir

echo "**********************************************"
echo "*** PRUEBAS DEL CLIENTE DFS COMPLETADAS ***"
echo "**********************************************"

# Limpiar archivos locales de prueba
rm archivo_vacio.txt archivo_pequeno.txt archivo_mediano.txt archivo_worm_v1.txt archivo_worm_v2.txt
rm desc_vacio.txt desc_pequeno.txt desc_mediano.txt desc_worm_v2.txt
rm desc_mediano_tf1.txt desc_mediano_tf2.txt 2>/dev/null # Ignorar error si no existen

./test.sh