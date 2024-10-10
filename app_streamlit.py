import pandas as pd
import streamlit as st
from funciones import (generar_ids_y_stock, generar_ids_y_stock_valor, generar_y_separar_mb52, 
                       filtrar_por_tipo_posicion, procesar_datos,graficar_proyeccion_pesca
)
from api import consultar_pesca
from proyeccion import realizar_proyeccion
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


# Configuración de la app
st.title("Análisis de Datos de SAP y API")
st.write("Sube los archivos Excel correspondientes para realizar el análisis.")

# Subida de archivos
uploaded_file_datasets = st.file_uploader("Subir datasets.xlsx", type="xlsx")
uploaded_file_mb51 = st.file_uploader("Subir MB51.xlsx", type="xlsx")
uploaded_file_mb52 = st.file_uploader("Subir MB52.xlsx", type="xlsx")
uploaded_file_me2n = st.file_uploader("Subir ME2N.xlsx", type="xlsx")

# Selectores de fechas con formato latinoamericano (día/mes/año)
inicio = st.date_input("Selecciona la fecha de inicio", datetime(2024, 4, 15), format="DD/MM/YYYY")
final = st.date_input("Selecciona la fecha final", datetime(2024, 7, 6), format="DD/MM/YYYY")

# Convertir fechas a formato requerido
inicio_str = inicio.strftime("%Y%m%d")
final_str = final.strftime("%Y%m%d")

# Control para tolerancia con slider (0% a 100%)
tolerancia = st.slider("Selecciona el valor de tolerancia (%)", 0, 100, 10) / 100

# Función para cargar datos en paralelo
def cargar_datos_en_paralelo(archivos):
    dfs = {}
    with ThreadPoolExecutor() as executor:
        future_to_sheet = {
            executor.submit(pd.read_excel, archivo, sheet_name='Sheet1'): key
            for key, archivo in archivos.items() if key != 'datasets'
        }
        sheets_future = executor.submit(
            lambda: {sheet: pd.read_excel(archivos['datasets'], sheet_name=sheet) for sheet in ['db_capacidad_instalada', 'db_cuota', 'db_ratios_planta_insumo', 'db_insumos']}
        )
        
        for future in future_to_sheet:
            key = future_to_sheet[future]
            dfs[key] = future.result()

        dfs.update(sheets_future.result())
    
    return dfs

# Procesar datos principales
def procesar_datos_principales(dfs):
    df_valor_centros = generar_ids_y_stock_valor(dfs['mb52'], 'general')
    
    dfs['mb51'] = generar_ids_y_stock(dfs['mb51'])
    df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general = generar_y_separar_mb52(dfs['mb52'])
    
    dfs['me2n'] = generar_ids_y_stock(dfs['me2n'])
    df_me2n_pt, df_me2n_oc = filtrar_por_tipo_posicion(dfs['me2n'])
    
    df_base = pd.merge(dfs['db_ratios_planta_insumo'], 
                       dfs['db_capacidad_instalada'][['id_localidad', 'cip', 'rendimiento', 'cobertura_ideal', 'maxima_descarga', 'cobertura_meta']],
                       on='id_localidad', how='left')
    
    df_base['stock_cobertura_ideal'] = (
        (df_base['ratio_nominal'] * df_base['maxima_descarga']) / df_base['rendimiento'] * df_base['cobertura_ideal']
    )
    
    df_consumo_total = dfs['mb51'].groupby(['id_localidad', 'id_insumo'])['Cantidad'].sum().abs().reset_index()
    
    # Consultar API
    df_datos, df_dias_produccion = consultar_pesca(inicio_str, final_str)
    
    # Agregar días de pesca
    df_consumo_total = pd.merge(df_consumo_total, df_dias_produccion[['id_localidad', 'dias_de_pesca']], 
                                on='id_localidad', how='left')
    df_consumo_total['consumo_diario'] = df_consumo_total['Cantidad'] / df_consumo_total['dias_de_pesca'].fillna(1)
    df_consumo_total['id_localidad_insumo'] = df_consumo_total['id_localidad'].astype(str) + df_consumo_total['id_insumo'].astype(str)
    
    return df_valor_centros, df_base, df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general, df_consumo_total, df_datos

# Ejecutar análisis cuando se haga clic en el botón
if st.button("Ejecutar análisis"):
    if uploaded_file_datasets and uploaded_file_mb51 and uploaded_file_mb52 and uploaded_file_me2n:
        with st.spinner('Procesando los datos, por favor espera...'):
            # Cargar datos subidos
            archivos_subidos = {
                'datasets': uploaded_file_datasets,
                'mb51': uploaded_file_mb51,
                'mb52': uploaded_file_mb52,
                'me2n': uploaded_file_me2n
            }
            dfs = cargar_datos_en_paralelo(archivos_subidos)
            
            # Procesar datos principales
            df_valor_centros, df_base, df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general, df_consumo_total, df_datos = procesar_datos_principales(dfs)
            
            # Procesar el resto de los datos
            df_resultado = procesar_datos(df_base, df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general, 
                                          df_consumo_total, dfs['db_insumos'])
            
            df_proyeccion_pesca = realizar_proyeccion(df_datos)
            
            # Mostrar resultados en Streamlit
            st.write("### Resultados del análisis de insumos")
            # Después de realizar la proyección
            graficar_proyeccion_pesca(df_proyeccion_pesca)
            
            # Guardar archivo Excel con los resultados
            with pd.ExcelWriter('resultados.xlsx') as writer:
                df_resultado.to_excel(writer, sheet_name='seguimiento_insumos', index=False)
                df_datos.to_excel(writer, sheet_name='seguimiento_pesca', index=False)
                df_valor_centros.to_excel(writer, sheet_name='valorizado_centros', index=False)
                df_proyeccion_pesca.to_excel(writer, sheet_name='proyeccion_pesca', index=False)

                # Hoja con la fecha y hora actual
                pd.DataFrame({'fecha': [datetime.now()]}).to_excel(writer, sheet_name='actualizado_el', index=False)
                pd.DataFrame({'tolerancia': [tolerancia]}).to_excel(writer, sheet_name='parametros', index=False)

            # Crear botón de descarga para el archivo Excel
            with open("resultados.xlsx", "rb") as file:
                st.download_button(label="Descargar resultados en Excel", data=file, file_name="resultados.xlsx")
    else:
        st.warning("Por favor, sube todos los archivos requeridos antes de ejecutar el análisis.")
