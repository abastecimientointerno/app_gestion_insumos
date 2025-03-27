import pandas as pd
import io
import json
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import os

# 📌 Configurar conexión
site_url = "https://tasaomega.sharepoint.com/sites/PD"
username = "sample"
password = "sample"
target_folder = "/sites/PD/Documentos compartidos/repositorio/app_reporte_insumos/db_data_insumos"

# 📌 Leer DataFrame
df_resultados = pd.read_excel("resultados.xlsx", sheet_name="seguimiento_insumos")

# 📌 Asegurar que todas las columnas estén como string y coincidan con el esquema
expected_columns = [
    "id_sap", "nombre_insumo", "id_insumo", "id_localidad", "valor_redondeo",
    "precio_unitario", "descripcion", "id_mix", "ratio_nominal", "familia",
    "familia_2", "cip", "rendimiento", "cobertura_ideal", "maxima_descarga",
    "cobertura_meta", "stock_cobertura_ideal", "id_localidad_insumo",
    "stock_libre_mas_calidad_produccion", "stock_libre_mas_calidad_transito",
    "stock_libre_mas_calidad_hub", "stock_libre_mas_calidad_general",
    "stock_libre_mas_calidad", "excedentes", "faltantes", "consumo_diario",
    "Cantidad", "dias_de_pesca", "cobertura_teorica_con_stock_general",
    "cobertura_real_general", "cobertura_teorica_con_stock_hub",
    "cobertura_real_hub", "cobertura_teorica_con_stock_transito",
    "cobertura_real_transito", "cobertura_teorica_con_stock_produccion",
    "cobertura_real_produccion", "temporada", "fecha_ejecucion"
]

# Filtrar solo las columnas esperadas y convertir todo a string
df_filtered = df_resultados[expected_columns].astype(str)

# 📌 Convertir DataFrame a lista de diccionarios
json_items = df_filtered.to_dict(orient='records')

# 📌 Crear la estructura completa según el esquema
json_structure = {
    "type": "array",
    "items": json_items
}

# 📌 Convertir a JSON con formato legible
json_data = json.dumps(json_structure, ensure_ascii=False, indent=2)
json_buffer = io.BytesIO(json_data.encode('utf-8'))

# 📌 Autenticación en SharePoint
ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))

# 📌 Subir archivo JSON a SharePoint
file_name = "data_insumos.json"
folder = ctx.web.get_folder_by_server_relative_url(target_folder)
folder.upload_file(file_name, json_buffer.getvalue()).execute_query()

print(f"✅ Archivo {file_name} subido a SharePoint correctamente en formato JSON.")