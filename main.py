from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
import io
from pathlib import Path
from datetime import datetime

app = FastAPI(title="Clean Data Tool API")

# 📂 Subida de archivo
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        ext = Path(file.filename).suffix.lower()

        # Detectar tipo de archivo
        if ext == ".csv":
            df = pd.read_csv(io.BytesIO(content))
        elif ext == ".json":
            df = pd.read_json(io.BytesIO(content))
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(io.BytesIO(content))
        else:
            return JSONResponse({"error": f"Formato no soportado: {ext}"}, status_code=400)

        # Guardamos temporalmente en memoria
        df_info = {
            "filas": len(df),
            "columnas": len(df.columns),
            "columnas_nombres": list(df.columns)
        }

        return {"mensaje": "Archivo cargado correctamente", "info": df_info}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# 🧼 Endpoint para limpiar nulos
@app.post("/clean/nulls")
async def clean_nulls(file: UploadFile = File(...)):
    try:
        content = await file.read()
        ext = Path(file.filename).suffix.lower()

        if ext == ".csv":
            df = pd.read_csv(io.BytesIO(content))
        elif ext == ".json":
            df = pd.read_json(io.BytesIO(content))
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(io.BytesIO(content))
        else:
            return JSONResponse({"error": f"Formato no soportado: {ext}"}, status_code=400)

        antes = len(df)
        df.dropna(inplace=True)
        despues = len(df)
        eliminadas = antes - despues

        # Guardar el resultado
        nombre = Path(file.filename).stem
        fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_salida = Path(f"data_limpia/{nombre}_limpio_{fecha}.csv")
        ruta_salida.parent.mkdir(exist_ok=True)
        df.to_csv(ruta_salida, index=False)

        return {"mensaje": "Limpieza completa", "filas_eliminadas": eliminadas, "archivo_salida": str(ruta_salida)}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
