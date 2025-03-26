import pandas as pd

df = pd.read_excel("resultados.xlsx", sheet_name="seguimiento_insumos")

print(df.describe())  # 📊 Resumen estadístico de todas las columnas