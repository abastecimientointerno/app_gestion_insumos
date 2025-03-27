import pandas as pd
import json

# Cargar el DataFrame desde el CSV (o usa df_resultados si ya lo tienes en memoria)
df = pd.read_excel("resultados.xlsx", sheet_name="seguimiento_insumos")

# Convertir el DataFrame a JSON
json_data = df.to_dict(orient="records")

# Guardar el JSON en un archivo para referencia
with open("ejemplo.json", "w", encoding="utf-8") as f:
    json.dump(json_data, f, indent=4, ensure_ascii=False)

print("✅ Archivo 'ejemplo.json' generado.")
