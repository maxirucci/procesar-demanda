# pip install psycopg2 win10toast plyer

import csv
from datetime import datetime
import psycopg2
import time
import os                               # Importar el módulo os
import warnings

# Evita mostrar una advertencia conocida de dependencia en win10toast.
warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated as an API.*",
    category=UserWarning,
)

from win10toast import ToastNotifier
from plyer import notification

def filtrar_data_values(archivo_valores, archivo_salida):
    irf_referencias = set()
    data = []

    itemsProcesados = 0
    itemsInsertados = 0

    conn = psycopg2.connect(
        dbname="historicos",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()

    # Registrar el tiempo de inicio
    tiempo_inicio = time.time()

    # Cargar los irn de referencia desde la base de datos
    irf_referencias = buscar_punto_referencia(cursor)


    # Filtrar el archivo data-values
    with open(archivo_valores, 'r', encoding="latin-1") as input_file:
        valores_ldr = csv.reader(input_file, delimiter=';')

        for item in valores_ldr:
            # # Validaciones básicas para evitar errores # #
            if not item:  # Saltar líneas vacías
                continue
            
            try:
                irn = int(item[0].strip())
            except ValueError:
                continue
            # # Fin de validaciones # #

            itemsProcesados += 1
    
            if irn in irf_referencias:
                itemsInsertados += 1
                data.append([item[0].strip(), item[1].strip(), item[2].strip()])
            
            # Insertar en la base de datos en bloques
            if itemsProcesados % 250 == 0:
                insertar_en_db(cursor, data)
                data = []  # Vaciar la lista después de insertar
            
            # Mostrar el avance
            mostrar_avance(itemsProcesados, tiempo_inicio)

    # Insertar cualquier dato restante
    if data:
        insertar_en_db(cursor, data)

        mostrar_avance(itemsProcesados, tiempo_inicio)

    # Confirmar la transacción y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

    # Agregar el nombre del archivo a la lista de archivos procesados
    with open(archivo_salida, 'a', encoding='latin-1') as output_file:
        output_file.write(f"{archivo_valores}\n")

    # Renombrar el archivo de valores agregando un "_" al principio del nombre
    ruta_directorio, nombre_archivo = os.path.split(archivo_valores)
    nuevo_nombre_archivo = os.path.join(ruta_directorio, f"_{nombre_archivo}")
    os.rename(archivo_valores, nuevo_nombre_archivo)

    # Calcular la duración del proceso
    duracion = time.time() - tiempo_inicio
    minutos, segundos = divmod(duracion, 60)

    print(f"Proceso finalizado ({int(minutos):02d}:{int(segundos):02d}):\n{itemsProcesados} líneas procesadas || {itemsInsertados} líneas insertadas.")




def mostrar_avance(itemsProcesados, tiempo_inicio):
    tiempo_parcial = time.time() - tiempo_inicio
    minutos, segundos = divmod(tiempo_parcial, 60)

    print(f"{int(itemsProcesados):07d} líneas procesadas... || Tiempo transcurrido: {int(minutos):02d}:{int(segundos):02d}\r", end='')


def buscar_punto_referencia(cursor):
    query = "SELECT irn FROM measurand_points;"
    cursor.execute(query)

    return {int(row[0]) for row in cursor.fetchall()}


def insertar_en_db(cursor, data):
    insert_query = """
    INSERT INTO lmea15minave (obe_irn, systime, value)
    VALUES (%s, to_timestamp(%s, 'YYYYMMDDHH24MISSMS'), %s)
    """

    cursor.executemany(insert_query, data)


def notificacion(mensaje, toaster):
    toaster.show_toast(
        "Procesamiento de Archivos",
        mensaje,
        duration=10**6,  # Un valor grande para que la notificación no se cierre automáticamente
        threaded=True
    )

    # notification.notify(
    #     title="Procesamiento de Archivos",
    #     message="Otra notificación",
    #     timeout=10
    # )




# Uso del script
if __name__ == "__main__":
    # archivo_ref = 'ref_measurand_20260127.csv'
    archivo_salida = 'archivos-procesados.txt'

    toaster = ToastNotifier()

    while True:
        try:
            archivo_valores = input("--> Ingrese el nombre del archivo de valores sin extensión (solo *.ldr): ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n--> Proceso cancelado por el usuario.")
            break

        if not archivo_valores:
            print("--> Debe ingresar un nombre de archivo.")
            continue
        
        try:
            filtrar_data_values((f"./historicosRaw/{archivo_valores}.ldr"), archivo_salida)
        except FileNotFoundError:
            print("--> Error al procesar el archivo. Verifique el nombre del archivo e intente nuevamente.")
            continue
        except KeyboardInterrupt:
            print("\n--> Proceso cancelado por el usuario.")
            break


        notificacion("¿Desea procesar otro archivo?", toaster)
        while True:
            try:
                continuar = input("\n--> ¿Desea procesar otro archivo? (s/n): ").strip().lower()
            except (KeyboardInterrupt, EOFError):
                print("\n--> Proceso cancelado por el usuario.")
                continuar = 'n'
                break

            if continuar in ('s', 'n'):
                break

            print("--> Respuesta inválida. Ingrese solo 's' o 'n'.")
        
        if continuar != 's':
            break