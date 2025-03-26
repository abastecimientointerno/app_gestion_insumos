from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential
import io
import pandas as pd
import getpass

# 📌 Configurar conexión
site_url = "https://tasaomega.sharepoint.com/sites/PD"
username = input("Correo de Office 365: ")
password = getpass.getpass("Contraseña: ")

ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
df_resultados = pd.read_excel("resultados.xlsx", sheet_name="seguimiento_insumos")
# 📌 Nombre del archivo y ubicación en SharePoint
file_name = "insumos_data.csv"
folder_path = "repositorio/app_reporte_insumos/db_data_insumos"  # Ruta en SharePoint
file_url = f"/sites/PD/Shared Documents/{folder_path}/{file_name}"

# 📌 Convertir DataFrame a CSV en memoria
csv_buffer = io.StringIO()
df_resultados.to_csv(csv_buffer, index=False)
csv_content = csv_buffer.getvalue().encode("utf-8")

# 📌 Subir archivo a la carpeta específica en SharePoint
folder = ctx.web.get_folder_by_server_relative_url(f"Shared Documents/{folder_path}")
file = folder.upload_file(file_name, csv_content)
ctx.execute_query()

print(f"✅ Archivo {file_name} subido a SharePoint en la carpeta '{folder_path}'.")
