# process-data

Script Python para carga masiva de archivos históricos de mediciones (`.ldr`) en una base de datos PostgreSQL local. Procesa automáticamente todos los archivos pendientes de la carpeta `historicosRaw/` en un único proceso batch sin intervención del usuario.

## ¿Qué hace?

Al ejecutarse, escanea `historicosRaw/` en busca de archivos `.ldr` sin procesar (los procesados tienen el prefijo `_`) y los carga en secuencia. Por cada archivo:

1. **Limpia** la tabla de staging `lmea15minave_raw`.
2. **Carga masiva** del contenido del archivo usando `COPY FROM STDIN` de PostgreSQL (saltando el encabezado hasta la marca `BEGINDATA`).
3. **Filtra y migra** los registros a la tabla final `lmea15minave` mediante un `INNER JOIN` con `measurand_points`, descartando los IRN que no son puntos de referencia.
4. **Registra** en `archivos-procesados.txt` la fecha, nombre de archivo y cantidad de filas insertadas.
5. **Limpia** la tabla de staging y **renombra** el archivo con el prefijo `_`.

Al finalizar todos los archivos, ejecuta `VACUUM ANALYZE` sobre `lmea15minave` y envía una notificación de escritorio.

## Requisitos

- Python 3.x
- PostgreSQL corriendo en `localhost:5432`

### Instalación de dependencias

```bash
pip install psycopg2 tqdm plyer
```

## Configuración de la base de datos

El script se conecta con los siguientes parámetros (hardcodeados en `process-data.py`):

| Parámetro  | Valor      |
|------------|------------|
| `dbname`   | `historicos` |
| `user`     | `postgres` |
| `password` | `postgres` |
| `host`     | `localhost`|
| `port`     | `5432`     |

### Tablas requeridas

- **`measurand_points`**: contiene los `irn` de referencia usados como filtro.
- **`lmea15minave_raw`**: tabla de staging para la carga masiva. Se trunca antes y después de cada archivo.  
  Columnas: `obe_irn`, `systime_raw` (texto), `value_val`.
- **`lmea15minave`**: tabla destino con los registros filtrados.  
  Columnas: `obe_irn`, `systime` (timestamp), `value`.

## Estructura del proyecto

```
process-data/
├── process-data.py          # Script principal
├── archivos-procesados.txt  # Log de archivos procesados
├── README.md
└── historicosRaw/           # Archivos .ldr a procesar
    └── LMEA15MINAVE_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.ldr
```

## Uso

Colocar los archivos `.ldr` en `historicosRaw/` y ejecutar:

```bash
python process-data.py
```

El script detecta automáticamente los archivos pendientes y los procesa todos. No requiere ninguna entrada del usuario.

**Ejemplo de salida:**

```
🚀 Se encontraron 3 archivos. Iniciando proceso masivo...

── Archivo 1 de 3 ──────────────────────
[1/6] Iniciando procesamiento de LMEA15MINAVE_20260125_130000_20260130_030000.ldr...
...
✅ ¡Éxito! 48320 registros procesados.

🧹 Optimizando base de datos (VACUUM ANALYZE)...
✨ Misión cumplida! Se procesaron 3 archivos.
📊 Total de registros nuevos: 142.500
⌛ Tiempo total transcurrido: 1m 23s
```

## Formato del archivo `.ldr`

Archivo de texto con encabezado de metadata seguido de la marca `BEGINDATA`. A partir de esa línea, el contenido es un CSV delimitado por `;` con codificación `latin-1`:

```
IRN;SYSTIME;VALUE
```

- `IRN`: identificador del punto de medición.
- `SYSTIME`: marca temporal en formato `YYYYMMDDHH24MISSMS`.
- `VALUE`: valor de la medición.

## Notas

- Un archivo `.ldr` renombrado con prefijo `_` ya fue procesado y no será tomado en una próxima ejecución.
- El archivo `archivos-procesados.txt` registra cada carga con timestamp y cantidad de filas.
- Si ocurre un error en un archivo, se hace rollback de esa transacción y el proceso continúa con el siguiente.
