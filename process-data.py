import psycopg2
import os
import time
import threading
import glob
from datetime import datetime
from tqdm import tqdm # Librería para la barra de progreso
from plyer import notification as plyer_notif



# Clase auxiliar para reportar el progreso de lectura a Postgres
class ProgressWrapper:
    def __init__(self, file_obj, pbar):
        self.file_obj = file_obj
        self.pbar = pbar

    def read(self, size):
        chunk = self.file_obj.read(size)
        self.pbar.update(len(chunk))
        return chunk
    
    # Algunos drivers de Postgres pueden pedir estas funciones
    def readline(self):
        line = self.file_obj.readline()
        self.pbar.update(len(line))
        return line


def mostrar_progreso_indeterminado(descripcion, stop_event, intervalo=0.2):
    # Barra sin total para tareas SQL largas donde no conocemos el avance real.
    with tqdm(total=None, desc=descripcion, unit="ticks", leave=False) as pbar:
        while not stop_event.wait(intervalo):
            pbar.update(1)


def formatear_duracion(segundos):
    segundos_totales = int(round(segundos))
    horas, resto = divmod(segundos_totales, 3600)
    minutos, segundos = divmod(resto, 60)

    if horas:
        return f"{horas}h {minutos}m {segundos}s"
    if minutos:
        return f"{minutos}m {segundos}s"
    return f"{segundos}s"


def procesar_ldr(ruta_input, archivo_log, conn):
    nombre_archivo = os.path.basename(ruta_input)

    if not os.path.exists(ruta_input):
        print(f"--> Error: El archivo {ruta_input} no existe.")
        return 0

    print(f"\n[1/6] Iniciando procesamiento de {nombre_archivo}...")

    cursor = conn.cursor()

    try:
        # 1. Limpiar tabla temporal
        print("[2/6] Limpiando tabla temporal lmea15minave_raw...")
        cursor.execute("TRUNCATE TABLE lmea15minave_raw;")

        # Obtenemos el tamaño del archivo para la barra (en bytes)
        file_size = os.path.getsize(ruta_input)
        print(f"[3/6] Preparando lectura del archivo ({file_size:,} bytes)...")
        
        # 2. Preparar el archivo (saltar encabezado hasta BEGINDATA)
        with open(ruta_input, 'r', encoding="latin-1") as f:
            # Saltamos la cabecera
            while True:
                line = f.readline()
                if not line:
                    raise ValueError("No se encontro la marca BEGINDATA en el archivo.")
                if "BEGINDATA" in line:
                    break

            # Solo mostramos progreso del bloque real de datos (despues de BEGINDATA)
            data_start_pos = f.tell()
            data_size = max(file_size - data_start_pos, 0)
            
            # 3. Carga masiva ultra rápida usando COPY
            # Usamos el puntero del archivo justo después de BEGINDATA
            sql_copy = """
                COPY lmea15minave_raw 
                FROM STDIN 
                WITH (FORMAT csv, DELIMITER ';');
            """

            print("[4/6] Cargando datos en PostgreSQL (COPY)...")
            with tqdm(
                total=data_size,
                unit="B",
                unit_scale=True,
                desc="COPY -> lmea15minave_raw",
                leave=False
            ) as pbar:
                wrapped_file = ProgressWrapper(f, pbar)
                cursor.copy_expert(sql_copy, wrapped_file)
            print("[4/6] Carga masiva finalizada.")

        # 4. Migración con filtrado (SQL INNER JOIN)
        # Esto es mucho más rápido que filtrar en Python
        sql_insert = """
            INSERT INTO lmea15minave (obe_irn, systime, value)
            SELECT 
                raw.obe_irn,
                to_timestamp(raw.systime_raw, 'YYYYMMDDHH24MISSMS'),
                raw.value_val
            FROM lmea15minave_raw raw
            INNER JOIN measurand_points mp ON raw.obe_irn = mp.irn;
        """
        stop_event = threading.Event()
        progress_thread = threading.Thread(
            target=mostrar_progreso_indeterminado,
            args=("[5/6] Migrando datos filtrados a tabla final", stop_event),
            daemon=True
        )
        progress_thread.start()

        try:
            cursor.execute(sql_insert)
        finally:
            stop_event.set()
            progress_thread.join()

        filas_insertadas = cursor.rowcount
        
        conn.commit()
        print("[5/6] Migracion completada y commit aplicado.")

        # 5. Log y Renombrar
        print("[6/6] Guardando log, limpiando temporal y renombrando archivo...")
        with open(archivo_log, 'a') as log:
            log.write(f"{datetime.now()}: {nombre_archivo}.ldr - {filas_insertadas} filas insertadas\n")
        
        # 6. Dejar limpia la tabla temporal
        cursor.execute("TRUNCATE TABLE lmea15minave_raw;")

        # Renombrar archivo (prefijo _ para marcarlo como procesado)
        carpeta = os.path.dirname(ruta_input)
        os.rename(ruta_input, os.path.join(carpeta, f"_{nombre_archivo}"))

        print(f"✅ ¡Éxito! {filas_insertadas} registros procesados.")
        return filas_insertadas

    except Exception as e:
        conn.rollback()
        print(f"❌ Error crítico en {nombre_archivo}: {e}")
        return 0
    finally:
        cursor.close()  # Solo cerramos el cursor; la conexion la maneja __main__


def mantenimiento_postgres(conn):
    # Función para realizar tareas de mantenimiento como VACUUM ANALYZE
    tiempo_inicio = time.perf_counter()

    try:
        # 1. Aseguramos que la transacción anterior esté cerrada
        conn.commit() 

        # 2. Activamos autocommit temporalmente (VACUUM lo requiere)
        conn.autocommit = True

        with conn.cursor() as maintenance_cursor:
            print("\n🧹 Optimizando base de datos (VACUUM ANALYZE)...")
            maintenance_cursor.execute("VACUUM ANALYZE lmea15minave;")
            print("✨ Mantenimiento finalizado.")

        # 3. Volvemos al estado normal
        conn.autocommit = False
    except Exception as e:
        print(f"❌ Error durante el mantenimiento: {e}")

    return time.perf_counter() - tiempo_inicio



if __name__ == "__main__":
    archivo_log = 'archivos-procesados.txt'

    # Buscar archivos .ldr que NO empiecen con "_" (los procesados ya tienen ese prefijo)
    archivos = sorted(
        f for f in glob.glob("./historicosRaw/*.ldr")
        if not os.path.basename(f).startswith("_")
    )

    if not archivos:
        print("──> No se encontraron archivos nuevos para procesar. ──────────────────────")
    else:
        print(f"🚀 Se encontraron {len(archivos)} archivos. Iniciando proceso masivo...")
        # 1. Iniciamos EL cronómetro global aquí
        tiempo_inicio_total = time.perf_counter()

        conn = psycopg2.connect(
            dbname="historicos",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        total_filas = 0

        try:
            for i, ruta in enumerate(archivos, 1):
                print(f"\n── Archivo {i} de {len(archivos)} ──────────────────────")
                filas = procesar_ldr(ruta, archivo_log, conn)
                total_filas += filas
        finally:
            # 2. El mantenimiento se hace dentro del bloque de tiempo
            mantenimiento_postgres(conn)
            conn.close()

            # 3. Calculamos la duración total de un solo golpe
            tiempo_total = time.perf_counter() - tiempo_inicio_total

        print(f"\n✨ ¡Misión cumplida! Se procesaron {len(archivos)} archivos.")
        print(f"📊 Total de registros nuevos: {total_filas:,}".replace(',', '.'))
        print(f"⌛ Tiempo total transcurrido: {formatear_duracion(tiempo_total)}")

        # Reemplazo de win10toast por plyer
        try:
            plyer_notif.notify(
                title="Proceso Terminado",
                message=f"Se cargaron {len(archivos)} archivos | {total_filas:,}".replace(',', '.') + " registros.",
                app_name="Postgres Loader",
                timeout=10 # Segundos que dura la notificación
            )
        except Exception as e:
            print(f"⚠️ No se pudo mostrar la notificación: {e}")