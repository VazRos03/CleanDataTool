import pandas as pd 
import numpy as np 
from pathlib import Path
import os 
import tqdm
import csv
from rich.console import Console
from rich.table import Table

console = Console()

# Mapeo de subcarpetas del proyecto -> rutas relativas
def read_subDir(base_path="."):
    """
    Muestra las subcarpetas dentro del directorio base y devuelve la ruta seleccionada.
    """
    base = Path(base_path)

    subdirs = [d for d in base.iterdir() if d.is_dir()]

    if not subdirs:
        console.print("[bold red]No se encontraron subcarpetas en el directorio base.[/bold red]")
        return None
    
    #tabla de subcarpetas a seleccionar
    table = Table(title="Subcarpetas disponibles", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Nombre", style="green")

    for i, d in enumerate(subdirs, 1):
        table.add_row(str(i), d.name)

    console.print(table)

    # Solicita la selección del usuario
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

#funcion para la seleccion de archivos de las subcarpetas 
def select_filesDir():
    """
    Llama a read_subDir() y muestra los archivos del directorio seleccionado.
    """
    selected_dir = read_subDir(".")

    if not selected_dir:
        return

    files = [f for f in selected_dir.iterdir() if f.is_file()]

    if not files:
        console.print(f"[yellow] No hay archivos en {selected_dir.name}[/yellow]")
        return

    table = Table(title=f"Archivos en '{selected_dir.name}'", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Archivo", style="magenta")

    for i, f in enumerate(files, 1):
        table.add_row(str(i), f.name)

    console.print(table)

    # Puedes devolver el archivo seleccionado si lo deseas:
    while True:
        try:
            choice = int(input("Selecciona el número del archivo (o 0 para salir): "))
            if choice == 0:
                console.print("[yellow]Saliendo...[/yellow]")
                return
            if 1 <= choice <= len(files):
                selected_file = files[choice - 1]
                console.print(f"\n[bold green] Archivo seleccionado:[/bold green] {selected_file.name}")
                return selected_file
            else:
                console.print("[bold red] Número fuera de rango, intenta de nuevo.[/bold red]")
        except ValueError:
            console.print("[bold red] Entrada no válida, ingresa un número.[/bold red]") 


def read_selected_file():
    """
    Lee el archivo previamente seleccionado y muestra las columnas del DataFrame.
    Detecta automáticamente separador en CSV y soporta JSON y Excel.
    """
    selected_file = select_filesDir()
    if not selected_file:
        return None

    ext = selected_file.suffix.lower()
    df = None

    # Lectura según extensión
    try:
        if ext == ".csv":
            # Detectar separador automáticamente
            with open(selected_file, 'r', encoding='utf-8') as f:
                sample = f.read(2048)  # leer un fragmento del archivo
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

    # Mostrar columnas en tabla enumerada
    table = Table(title=f"Columnas del archivo '{selected_file.name}'", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Nombre de columna", style="green")

    for i, col in enumerate(df.columns, 1):
        table.add_row(str(i), col)

    console.print(table)

    console.print(f"[green]DataFrame cargado con {len(df)} filas y {len(df.columns)} columnas[/green]")

    return df


#### llamando a la función solo si se ejecuta directamente
if __name__ == "__main__":
    df = read_selected_file()

    if df is not None:
        console.print(f"[green]DataFrame cargado con {len(df)} filas y {len(df.columns)} columnas[/green]")