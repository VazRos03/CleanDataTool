#!/usr/bin/env python3
# limpieza_spark.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType
from pathlib import Path
from datetime import datetime
import pandas as pd
from tkinter import Tk, filedialog
from rich.console import Console
from rich.table import Table
import csv
import re
import numpy as np

console = Console()

# ---------------------------
# Inicializar Spark (local)
# ---------------------------
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("HerramientaLimpiezaSpark_Local") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()


# ---------------------------
# Utilidades: selección de carpetas/archivos (igual que antes)
# ---------------------------
def read_subDir(base_path="."):
    base = Path(base_path)
    subdirs = [d for d in base.iterdir() if d.is_dir()]
    if not subdirs:
        console.print("[bold red]No se encontraron subcarpetas en el directorio base.[/bold red]")
        return None

    table = Table(title="Subcarpetas disponibles", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Nombre", style="green")
    for i, d in enumerate(subdirs, 1):
        table.add_row(str(i), d.name)
    console.print(table)

    while True:
        try:
            choice = int(input("Selecciona el número de la carpeta: "))
            if 1 <= choice <= len(subdirs):
                selected_dir = subdirs[choice - 1]
                console.print(f"\n[bold green] Carpeta seleccionada:[/bold green] {selected_dir.name}\n")
                return selected_dir
            else:
                console.print("[bold red] Número fuera de rango, intenta de nuevo.[/bold red]")
        except ValueError:
            console.print("[bold red] Entrada no válida, ingresa un número.[/bold red]")


def select_filesDir():
    selected_dir = read_subDir(".")
    if not selected_dir:
        return None

    files = [f for f in selected_dir.iterdir() if f.is_file()]
    if not files:
        console.print(f"[yellow] No hay archivos en {selected_dir.name}[/yellow]")
        return None

    table = Table(title=f"Archivos en '{selected_dir.name}'", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Archivo", style="magenta")
    for i, f in enumerate(files, 1):
        table.add_row(str(i), f.name)
    console.print(table)

    while True:
        try:
            choice = int(input("Selecciona el número del archivo (o 0 para salir): "))
            if choice == 0:
                console.print("[yellow]Saliendo...[/yellow]")
                return None
            if 1 <= choice <= len(files):
                selected_file = files[choice - 1]
                console.print(f"\n[bold green] Archivo seleccionado:[/bold green] {selected_file.name}")
                return selected_file
            else:
                console.print("[bold red] Número fuera de rango, intenta de nuevo.[/bold red]")
        except ValueError:
            console.print("[bold red] Entrada no válida, ingresa un número.[/bold red]")


# ---------------------------
# Lectura de archivo (selección)
# ---------------------------
def read_selected_file():
    selected_file = select_filesDir()
    if not selected_file:
        return None, None

    ext = selected_file.suffix.lower()
    df = None

    try:
        if ext == ".csv":
            # detectar separador como en tu original
            with open(selected_file, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(2048)
                dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '|', ':', '\t'])
                sep_detected = dialect.delimiter
            df = spark.read.option("header", True).option("inferSchema", True).option("sep", sep_detected).csv(str(selected_file))
            console.print(f"[blue]Separador detectado automáticamente:[/blue] '{sep_detected}'")
        elif ext == ".json":
            df = spark.read.option("multiline", "true").json(str(selected_file))
        elif ext in [".xls", ".xlsx"]:
            # usar pandas para leer excel y convertir a spark (igual que sugeriste previamente)
            pdf = pd.read_excel(str(selected_file))
            df = spark.createDataFrame(pdf)
        else:
            console.print(f"[red]Formato no soportado: {ext}[/red]")
            return None, None
    except Exception as e:
        console.print(f"[red]Error al leer el archivo: {e}[/red]")
        return None, None

    # Mostrar columnas con rich
    table = Table(title=f"Columnas del archivo '{selected_file.name}'", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Nombre de columna", style="green")
    for i, col in enumerate(df.columns, 1):
        table.add_row(str(i), col)
    console.print(table)

    # contar filas (puede ser costoso, pero mantengo la lógica original)
    try:
        filas = df.count()
    except:
        filas = "N/A"

    console.print(f"[green]DataFrame cargado con {filas} filas y {len(df.columns)} columnas[/green]")
    return df, selected_file


# ---------------------------
# Funciones de limpieza / inspección (adaptadas a Spark)
# Todas devuelven df cuando hacen mutación
# ---------------------------

def tipo_datos(df, columna):
    """
    Emula la inspección de tipos por valor del pandas original:
    - muestrea hasta 10k valores y cuenta tipos Python de los elementos.
    """
    # Tomamos una muestra límite para no traer todo
    max_take = 10000
    sample_vals = df.select(columna).limit(max_take).rdd.map(lambda r: r[0]).collect()
    tipo_map = {
        int: "int",
        float: "float",
        str: "str",
        bool: "bool",
        type(None): "NoneType"
    }
    counts = {}
    for v in sample_vals:
        t = type(v)
        label = tipo_map.get(t, str(t))
        counts[label] = counts.get(label, 0) + 1

    table = Table(title=f"Tipos de datos de la columna '{columna}' (muestra hasta {max_take})", show_lines=True)
    table.add_column("Tipo de dato", style="cyan")
    table.add_column("Cantidad", justify="center", style="green")
    for k, v in counts.items():
        table.add_row(k, str(v))
    console.print(table)
    return df


def eliminar_columna(df, columna):
    df2 = df.drop(columna)
    console.print(f"[red]Columna '{columna}' eliminada[/red]")
    return df2


def transformar_columna(df, columna):
    opciones = {"1":"int", "2":"float", "3":"str", "4":"bool", "5":"date"}
    console.print("Opciones de transformación: 1=int, 2=float, 3=str, 4=bool, 5=date")
    choice = input("Selecciona tipo de transformación: ")

    if choice in opciones:
        tipo = opciones[choice]
        try:
            if tipo == "date":
                # Intentamos parsear con to_timestamp, si falla quedará null
                df2 = df.withColumn(columna, F.to_timestamp(F.col(columna)))
                console.print(f"[green]Columna '{columna}' convertida a fecha (timestamp)[/green]")
                return df2
            elif tipo == "int":
                df2 = df.withColumn(columna, F.col(columna).cast("long"))
            elif tipo == "float":
                df2 = df.withColumn(columna, F.col(columna).cast("double"))
            elif tipo == "str":
                df2 = df.withColumn(columna, F.col(columna).cast("string"))
            elif tipo == "bool":
                # convertir a bool: 'true','1' -> True, else False/NULL
                df2 = df.withColumn(columna,
                                    F.when(F.lower(F.col(columna).cast(StringType())).isin("true","1","t","yes","y"), True)
                                     .when(F.col(columna).isNull(), None)
                                     .otherwise(False))
            console.print(f"[green]Columna '{columna}' convertida a {tipo}[/green]")
            return df2
        except Exception as e:
            console.print(f"[red]Error al convertir la columna: {e}[/red]")
            return df
    else:
        console.print("[red]Opción no válida[/red]")
        return df


def cantidad_nulos(df):
    """
    Muestra la cantidad total de valores nulos por columna.
    """
    console.print("\n[yellow]Cantidad de valores nulos por columna:[/yellow]")
    # generamos expresiones para conteo de nulos por columna
    exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    null_counts = df.agg(*exprs).collect()[0].asDict()
    total = sum(null_counts.values())
    for columna, cantidad in null_counts.items():
        console.print(f" - {columna}: {cantidad} nulos")
    console.print(f"\n[bold cyan]Total de valores nulos en el DataFrame: {total}[/bold cyan]")
    return df


def eliminar_nulos(df):
    """
    Elimina todas las filas que contengan valores nulos en cualquier columna.
    """
    antes = df.count()
    df2 = df.na.drop()
    despues = df2.count()
    eliminadas = antes - despues
    console.print(f"[red]Se eliminaron {eliminadas} filas que contenían valores nulos[/red]")
    console.print(f"[green]Total de filas restantes: {despues}[/green]")
    return df2


def renombrar_columna(df, columna):
    nuevo_nombre = input(f"Ingresa el nuevo nombre para la columna '{columna}': ")
    df2 = df.withColumnRenamed(columna, nuevo_nombre)
    console.print(f"[green]Columna renombrada a '{nuevo_nombre}'[/green]")
    return df2


def mostrar_head(df, n=10):
    # Usamos toPandas() para mostrar con rich (mismo comportamiento visual)
    pd_head = df.limit(n).toPandas()
    table = Table(title=f"Primeros {n} registros del DataFrame", show_lines=True)
    table.add_column("Índice", justify="center", style="magenta")
    for col in pd_head.columns:
        table.add_column(str(col), style="green")
    for idx, row in pd_head.iterrows():
        table.add_row(str(idx), *[str(row[col]) for col in pd_head.columns])
    console.print(table)
    return df


def extraer_numeros(df, columna):
    """
    Extrae números (enteros, decimales o porcentajes) de una columna que contiene texto mezclado.
    Implementación vectorizada en Spark con regexp_extract.
    """
    try:
        # Extraer primer número con regexp
        # Capturamos número decimal opcionalmente con separador punto
        num_expr = F.regexp_extract(F.col(columna).cast(StringType()), r'(\d+(?:\.\d+)?)', 1)
        # Detectar si contiene '%' para dividir entre 100
        contains_pct = F.when(F.col(columna).cast(StringType()).contains('%'), True).otherwise(False)
        df2 = df.withColumn("_num_tmp", F.when(num_expr == "", None).otherwise(num_expr.cast(DoubleType())))
        df2 = df2.withColumn("_num_tmp", F.when(contains_pct & F.col("_num_tmp").isNotNull(), F.col("_num_tmp")/100).otherwise(F.col("_num_tmp")))
        df2 = df2.withColumn(columna, F.col("_num_tmp")).drop("_num_tmp")
        console.print(f"[green]Números extraídos correctamente de la columna '{columna}'.[/green]")
        mostrar_head(df2)
        return df2
    except Exception as e:
        console.print(f"[red]Error al extraer números: {e}[/red]")
        return df


def separar_valores(df, columna):
    """
    Separa valores en una columna usando un separador indicado por el usuario.
    Crea una nueva columna 'nueva_columna' y reemplaza la original con la parte izquierda.
    """
    separador = input("Ingresa el separador de los valores (por ejemplo ':' o '-'): ").strip()
    # Validar existencia del separador en al menos un valor (muestreo)
    has_sep = df.select(F.col(columna).cast(StringType()).rlike(re.escape(separador))).limit(1).collect()
    if not has_sep:
        console.print(f"[red]No se encontró el separador '{separador}' en los valores de la columna '{columna}'.[/red]")
        return df
    try:
        parts = F.split(F.col(columna).cast(StringType()), re.escape(separador), 2)
        df2 = df.withColumn("nueva_columna", parts.getItem(1)).withColumn(columna, parts.getItem(0))
        console.print(f"[green]Columna '{columna}' separada correctamente. Nueva columna creada: 'nueva_columna'[/green]")
        mostrar_head(df2)
        return df2
    except Exception as e:
        console.print(f"[red]Error al separar valores: {e}[/red]")
        return df


def detectar_patrones(df, columna):
    """
    Detecta patrones básicos: valores únicos, frecuencia de los más comunes.
    """
    console.print(f"[cyan]Análisis de patrones para la columna '{columna}':[/cyan]")
    # número de valores únicos
    uniques = df.select(columna).distinct().count()
    console.print(f"- Valores únicos: {uniques}")
    console.print(f"- Valores más frecuentes:")
    top_values = df.groupBy(columna).count().orderBy(F.col("count").desc()).limit(10).collect()
    table = Table(show_lines=True)
    table.add_column("Valor", style="green")
    table.add_column("Frecuencia", justify="center", style="cyan")
    for row in top_values:
        table.add_row(str(row[0]), str(row[1]))
    console.print(table)
    return df


def correlaciones(df):
    """
    Muestra correlaciones entre columnas numéricas.
    Para mantener la misma visualización, convertimos (cuando es razonable) a pandas para calcular corr.
    """
    # identificar columnas numéricas según inferencia de spark schema
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (DoubleType,))]
    # también considerar integer/long
    # Spark types for integer/long are LongType/IntegerType; usaremos casting por nombre
    # Para ser pragmáticos, intentamos convertir cualquier columna a double temporalmente si parece numérica en muestreo
    # Construimos pd_df con columnas que se puedan convertir
    if not numeric_cols:
        # intentemos detectar columnas que parezcan numéricas por muestreo
        sample = df.limit(1000).toPandas()
        possible = []
        for col in sample.columns:
            try:
                pd.to_numeric(sample[col].dropna().iloc[:100])
                possible.append(col)
            except:
                pass
        numeric_cols = possible

    if not numeric_cols:
        console.print("[yellow]No hay columnas numéricas para correlación[/yellow]")
        return df

    # Convertir a pandas solo con las columnas numéricas (riesgo de memory si DF gigante)
    pd_numeric = df.select(*numeric_cols).toPandas()
    corr = pd_numeric.corr()

    table = Table(title="Matriz de correlación", show_lines=True)
    table.add_column("Columna", style="green")
    for col in corr.columns:
        table.add_column(col, justify="center", style="cyan")
    for idx in corr.index:
        table.add_row(str(idx), *[f"{v:.2f}" for v in corr.loc[idx]])
    console.print(table)
    return df


# ---------------------------
# Guardado (manteniendo estructura de carpetas)
# ---------------------------
def guardar_dataframe(df, archivo_original):
    base_dir = Path("data_limpia")
    base_dir.mkdir(exist_ok=True)

    csv_dir = base_dir / "csv_limpia"
    json_dir = base_dir / "json_limpia"
    excel_dir = base_dir / "excel_limpia"

    csv_dir.mkdir(exist_ok=True)
    json_dir.mkdir(exist_ok=True)
    excel_dir.mkdir(exist_ok=True)

    ext = archivo_original.suffix.lower()
    nombre_base = archivo_original.stem

    fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        if ext == ".csv":
            ruta_guardado = csv_dir / f"{nombre_base}_limpio_{fecha_actual}.csv"
            # coalesce a 1 para obtener un único csv (como en pandas)
            df.coalesce(1).write.option("header", True).option("sep", ",").mode("overwrite").csv(str(ruta_guardado.parent / ruta_guardado.stem))
            # El path anterior genera una carpeta con part-*.csv: mover/renombrar si quieres un solo archivo .csv
            console.print(f"[green]Archivo guardado correctamente en carpeta: {ruta_guardado.parent / ruta_guardado.stem} (part-*.csv)[/green]")

        elif ext == ".json":
            ruta_guardado = json_dir / f"{nombre_base}_limpio_{fecha_actual}.json"
            df.coalesce(1).write.mode("overwrite").json(str(ruta_guardado.parent / ruta_guardado.stem))
            console.print(f"[green]Archivo guardado correctamente en carpeta: {ruta_guardado.parent / ruta_guardado.stem} (part-*.json)[/green]")

        elif ext in [".xls", ".xlsx"]:
            ruta_guardado = excel_dir / f"{nombre_base}_limpio_{fecha_actual}.xlsx"
            # convertir a pandas y guardar en excel
            pd_df = df.toPandas()
            pd_df.to_excel(ruta_guardado, index=False)
            console.print(f"[green]Archivo guardado correctamente en: {ruta_guardado}[/green]")
        else:
            console.print(f"[red]No se puede guardar archivo con extensión {ext}[/red]")
            return
    except Exception as e:
        console.print(f"[red]Error al guardar el archivo: {e}[/red]")


# ---------------------------
# Menú por columna y orquestador principal
# ---------------------------
def menu_columna(df, columna):
    opciones = {
        "1": tipo_datos,
        "2": eliminar_columna,
        "3": transformar_columna,
        "4": cantidad_nulos,
        "5": eliminar_nulos,
        "6": renombrar_columna,
        "7": mostrar_head,
        "8": extraer_numeros,
        "9": separar_valores
    }

    while True:
        console.print(f"\nOpciones para la columna '{columna}':")
        console.print("1. Tipo de datos")
        console.print("2. Eliminar columna")
        console.print("3. Transformación de datos")
        console.print("4. Cantidad de datos nulos")
        console.print("5. Eliminar datos nulos")
        console.print("6. Renombrar columna")
        console.print("7. Mostrar primeros registros del DataFrame")
        console.print("8. Extraer números de texto")
        console.print("9. Separar valores en nueva columna")
        console.print("0. Volver al menú de columnas")

        choice = input("Selecciona una opción: ")
        if choice == "0":
            break
        elif choice in opciones:
            # atención: algunas funciones retornan df nuevo
            if choice == "7":
                df = opciones[choice](df)
            else:
                resultado = opciones[choice](df, columna)
                # si la función devolvió un DataFrame lo usamos
                if resultado is not None:
                    df = resultado
        else:
            console.print("[red]Opción no válida[/red]")
    return df


def orquestador_lim(df, archivo_original):
    """
    Orquestador principal para limpieza y análisis básico del DataFrame.
    Maneja la referencia al DataFrame de Spark (se reasigna cuando se muta).
    """
    while True:
        console.print(f"\n[bold blue]=== Archivo actual: {archivo_original.name} ===[/bold blue]")

        table = Table(title="Columnas disponibles", show_lines=True)
        table.add_column("Número", justify="center", style="cyan")
        table.add_column("Nombre de columna", style="green")
        for i, col in enumerate(df.columns, 1):
            table.add_row(str(i), col)
        console.print(table)

        console.print("\n[bold cyan]=== Menú principal ===[/bold cyan]")
        console.print("1. Seleccionar columna para limpieza")
        console.print("2. Detectar patrones en una columna")
        console.print("3. Mostrar correlaciones entre columnas numéricas")
        console.print("G. Guardar DataFrame limpio")
        console.print("0. Salir del orquestador")

        choice = input("Selecciona una opción: ").strip().lower()

        if choice == "0":
            console.print("[yellow]Saliendo del orquestador[/yellow]")
            break

        elif choice == "1":
            try:
                num = int(input("Selecciona el número de la columna: "))
                if 1 <= num <= len(df.columns):
                    columna = df.columns[num - 1]
                    df = menu_columna(df, columna)
                else:
                    console.print("[red]Número fuera de rango[/red]")
            except ValueError:
                console.print("[red]Entrada no válida[/red]")

        elif choice == "2":
            try:
                num = int(input("Selecciona el número de la columna para detectar patrones: "))
                if 1 <= num <= len(df.columns):
                    columna = df.columns[num - 1]
                    df = detectar_patrones(df, columna)
                else:
                    console.print("[red]Número fuera de rango[/red]")
            except ValueError:
                console.print("[red]Entrada no válida[/red]")

        elif choice == "3":
            df = correlaciones(df)

        elif choice == "g":
            guardar_dataframe(df, archivo_original)

        else:
            console.print("[red]Opción no válida[/red]")
    return df


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    df, archivo = read_selected_file()
    if df is not None:
        archivo = Path(archivo)
        df = orquestador_lim(df, archivo)
        console.print("[green]Proceso de limpieza finalizado[/green]")
    else:
        console.print("[red]No se cargó ningún archivo. Terminando programa.[/red]")

    # detener spark
    try:
        spark.stop()
    except:
        pass