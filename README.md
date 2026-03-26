# process-data

Script Python para procesar archivos históricos de mediciones (`.ldr`) e insertarlos en una base de datos PostgreSQL local.

## ¿Qué hace?

1. Solicita al usuario el nombre de un archivo `.ldr` ubicado en `historicosRaw/`.
2. Lee los puntos de medición válidos desde la tabla `measurand_points` de la base de datos.
3. Filtra las líneas del archivo, conservando solo los registros cuyo `IRN` coincida con un punto de referencia.
4. Inserta los registros filtrados en la tabla `lmea15minave` en bloques de 250 filas.
5. Muestra el progreso en consola (líneas procesadas y tiempo transcurrido).
6. Al finalizar:
   - Registra el archivo en `archivos-procesados.txt`.
   - Renombra el archivo original agregando un prefijo `_` para marcarlo como procesado.
   - Envía una notificación de escritorio (Windows).
7. Pregunta si se desea procesar otro archivo; de lo contrario, finaliza.

## Requisitos

- Python 3.x
- PostgreSQL corriendo en `localhost:5432`
- Sistema operativo Windows (por la dependencia `win10toast`)

### Instalación de dependencias

```bash
pip install psycopg2 win10toast plyer
```

## Configuración de la base de datos

El script se conecta con los siguientes parámetros (hardcodeados en `process-data.py`):

| Parámetro | Valor       |
|-----------|-------------|
| `dbname`  | `historicos` |
| `user`    | `postgres`  |
| `password`| `postgres`  |
| `host`    | `localhost` |
| `port`    | `5432`      |

### Tablas requeridas

- **`measurand_points`**: contiene los `irn` de referencia usados como filtro.
- **`lmea15minave`**: tabla destino donde se insertan los registros procesados. Columnas esperadas: `obe_irn`, `systime`, `value`.

## Estructura del proyecto

```
process-data/
├── process-data.py          # Script principal
├── archivos-procesados.txt  # Log de archivos ya procesados
├── README.md
└── historicosRaw/           # Carpeta con los archivos .ldr a procesar
    └── LMEA15MINAVE_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.ldr
```

## Uso

```bash
python process-data.py
```

El script solicitará el nombre del archivo **sin extensión**:

```
--> Ingrese el nombre del archivo de valores sin extensión (solo *.ldr): LMEA15MINAVE_20260125_130000_20260130_030000
```

El archivo debe estar ubicado en la carpeta `historicosRaw/`. Una vez procesado, será renombrado con el prefijo `_` (por ejemplo, `_LMEA15MINAVE_20260125_130000_20260130_030000.ldr`).

## Formato del archivo `.ldr`

Archivo de texto delimitado por `;`, codificación `latin-1`. Cada línea representa una medición con la siguiente estructura:

```
IRN;SYSTIME;VALUE
```

- `IRN`: identificador del punto de medición.
- `SYSTIME`: marca temporal en formato `YYYYMMDDHH24MISSMS`.
- `VALUE`: valor de la medición.

## Notas

- Los archivos ya procesados quedan listados en `archivos-procesados.txt`.
- El prefijo `_` en el nombre de un archivo `.ldr` indica que ya fue procesado.
- Presionar `Ctrl+C` en cualquier momento cancela el proceso de forma segura.
