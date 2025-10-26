import pandas as pd 
import numpy as np 
from pathlib import Path
import os 
import tqdm
import csv
from rich.console import Console
from rich.table import Table

console = Console()

# Funciones de selección de archivos

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


# Función para leer el archivo

def read_selected_file():
    selected_file = select_filesDir()
    if not selected_file:
        return None

    ext = selected_file.suffix.lower()
    df = None

    try:
        if ext == ".csv":
            with open(selected_file, 'r', encoding='utf-8') as f:
                sample = f.read(2048)
                dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '|', ':'])
                sep_detected = dialect.delimiter
            df = pd.read_csv(selected_file, sep=sep_detected)
        elif ext == ".json":
            df = pd.read_json(selected_file)
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(selected_file)
        else:
            console.print(f"[red]Formato no soportado: {ext}[/red]")
            return None
    except Exception as e:
        console.print(f"[red]Error al leer el archivo: {e}[/red]")
        return None

    table = Table(title=f"Columnas del archivo '{selected_file.name}'", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Nombre de columna", style="green")
    for i, col in enumerate(df.columns, 1):
        table.add_row(str(i), col)
    console.print(table)
    console.print(f"[green]DataFrame cargado con {len(df)} filas y {len(df.columns)} columnas[/green]")

    return df

# Funciones de limpieza por columna

def tipo_datos(df, columna):
    tipo_map = {
        int: "int",
        float: "float",
        str: "str",
        bool: "bool",
        type(None): "NoneType"
    }

    tipos = df[columna].apply(lambda x: tipo_map.get(type(x), str(type(x)))).value_counts()

    table = Table(title=f"Tipos de datos de la columna '{columna}'", show_lines=True)
    table.add_column("Tipo de dato", style="cyan")
    table.add_column("Cantidad", justify="center", style="green")

    for t, c in tipos.items():
        table.add_row(t, str(c))

    console.print(table)


def eliminar_columna(df, columna):
    df.drop(columns=[columna], inplace=True)
    console.print(f"[red]Columna '{columna}' eliminada[/red]")

def transformar_columna(df, columna):
    opciones = {"1":"int", "2":"float", "3":"str", "4":"bool", "5":"date"}
    console.print("Opciones de transformación: 1=int, 2=float, 3=str, 4=bool, 5=date")
    choice = input("Selecciona tipo de transformación: ")

    if choice in opciones:
        tipo = opciones[choice]
        try:
            if tipo == "date":
                df[columna] = pd.to_datetime(df[columna], errors='coerce')
                console.print(f"[green]Columna '{columna}' convertida a fecha (datetime)[/green]")
            else:
                df[columna] = df[columna].astype(tipo)
                console.print(f"[green]Columna '{columna}' convertida a {tipo}[/green]")
        except Exception as e:
            console.print(f"[red]Error al convertir la columna: {e}[/red]")


def cantidad_nulos(df, columna):
    nulos = df[columna].isnull().sum()
    console.print(f"[yellow]Columna '{columna}' tiene {nulos} datos nulos[/yellow]")

def eliminar_nulos(df, columna):
    antes = len(df)
    df.dropna(subset=[columna], inplace=True)
    despues = len(df)
    console.print(f"[red]Se eliminaron {antes - despues} filas con nulos en '{columna}'[/red]")

def renombrar_columna(df, columna):
    nuevo_nombre = input(f"Ingresa el nuevo nombre para la columna '{columna}': ")
    df.rename(columns={columna:nuevo_nombre}, inplace=True)
    console.print(f"[green]Columna renombrada a '{nuevo_nombre}'[/green]")

def mostrar_head(df, n=10):
    table = Table(title=f"Primeros {n} registros del DataFrame", show_lines=True)
    # Agregar columna para índice
    table.add_column("Índice", justify="center", style="magenta")
    # Agregar columnas del dataframe
    for col in df.columns:
        table.add_column(col, style="green")

    # Iterar por las primeras n filas
    for idx, row in df.head(n).iterrows():
        table.add_row(str(idx), *[str(row[col]) for col in df.columns])

    console.print(table)

#funcion para guardar Dataframe
def guardar_dataframe(df, archivo_original):
    """
    Guarda el DataFrame en 'data_limpia' dentro de la subcarpeta correspondiente
    según la extensión del archivo, agregando '_limpio' al nombre.
    """
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

    try:
        if ext == ".csv":
            ruta_guardado = csv_dir / f"{nombre_base}_limpio.csv"
            df.to_csv(ruta_guardado, index=False)
        elif ext == ".json":
            ruta_guardado = json_dir / f"{nombre_base}_limpio.json"
            df.to_json(ruta_guardado, orient="records", indent=4)
        elif ext in [".xls", ".xlsx"]:
            ruta_guardado = excel_dir / f"{nombre_base}_limpio.xlsx"
            df.to_excel(ruta_guardado, index=False)
        else:
            console.print(f"[red]No se puede guardar archivo con extensión {ext}[/red]")
            return

        console.print(f"[green]Archivo guardado correctamente en: {ruta_guardado}[/green]")

    except Exception as e:
        console.print(f"[red]Error al guardar el archivo: {e}[/red]")



# Menú interactivo por columna
def menu_columna(df, columna):
    opciones = {
        "1": tipo_datos,
        "2": eliminar_columna,
        "3": transformar_columna,
        "4": cantidad_nulos,
        "5": eliminar_nulos,
        "6": renombrar_columna,
        "7": mostrar_head
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
        console.print("0. Volver al menú de columnas")

        choice = input("Selecciona una opción: ")
        if choice == "0":
            break
        elif choice in opciones:
            # Para la opción 7 no necesita columna específica
            if choice == "7":
                opciones[choice](df)
            else:
                opciones[choice](df, columna)
        else:
            console.print("[red]Opción no válida[/red]")

# Orquestador principal -> este orquesta a las funciones de limpieza basicas

def orquestador_lim(df, archivo_original):
    while True:
        table = Table(title="Columnas disponibles", show_lines=True)
        table.add_column("Número", justify="center", style="cyan")
        table.add_column("Nombre de columna", style="green")
        for i, col in enumerate(df.columns, 1):
            table.add_row(str(i), col)
        console.print(table)
        console.print("0. Salir del orquestador")
        console.print("G. Guardar DataFrame limpio")

        choice = input("Selecciona el número de la columna o 'G' para guardar: ").strip().lower()

        if choice == "0":
            console.print("[yellow]Saliendo del orquestador[/yellow]")
            break
        elif choice == "g":
            guardar_dataframe(df, archivo_original)
        else:
            try:
                choice = int(choice)
                if 1 <= choice <= len(df.columns):
                    columna = df.columns[choice-1]
                    menu_columna(df, columna)
                else:
                    console.print("[red]Número fuera de rango[/red]")
            except ValueError:
                console.print("[red]Entrada no válida[/red]")

if __name__ == "__main__":
    archivo = select_filesDir()
    if archivo:
        df = read_selected_file()
        if df is not None:
            orquestador_lim(df, archivo)
            console.print("[green]Proceso de limpieza finalizado[/green]")