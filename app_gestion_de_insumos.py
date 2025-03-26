import pandas as pd
import streamlit as st
from modules.utils_gestion_de_insumos import (
    generar_ids_y_stock, generar_ids_y_stock_valor, 
    generar_y_separar_mb52, procesar_datos, 
    consultar_pesca, realizar_proyeccion
)
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pytz

class GestionInsumos:
    """
    Clase principal para la aplicación de gestión de insumos.
    Maneja la carga, procesamiento y visualización de datos.
    """
    
    def __init__(self):
        """Inicializa la configuración de la aplicación Streamlit."""
        self.configurar_pagina()
        self.cargar_archivos()
        self.configurar_parametros()
    
    def configurar_pagina(self):
        """Configura el título y el estilo de la página Streamlit."""
        st.title("Gestión de Insumos")
        st.subheader("Carga de archivos y parámetros:")
    
    def cargar_archivos(self):
        """
        Crea widgets para cargar archivos Excel.
        Soporta datasets, MB51 y MB52.
        """
        self.uploaded_file_datasets = st.file_uploader("Cargar archivo :red[datasets]:", type="xlsx")
        self.uploaded_file_mb51 = st.file_uploader("Cargar archivo :red[MB51]:", type="xlsx")
        self.uploaded_file_mb52 = st.file_uploader("Cargar archivo :red[MB52]:", type="xlsx")
    
    def configurar_parametros(self):
        """
        Configura los parámetros de fecha y tolerancia.
        Permite al usuario seleccionar rangos de fecha y porcentaje de tolerancia.
        """
        fecha_defecto_inicio = datetime(2024, 4, 15)
        fecha_defecto_final = datetime(2024, 5, 6)
        
        self.inicio = st.date_input(
            ":red[Selecciona la fecha de inicio de la pesca]:", 
            fecha_defecto_inicio, 
            format="DD/MM/YYYY"
        )
        self.final = st.date_input(
            ":red[Selecciona la fecha de cierre de la pesca]:", 
            fecha_defecto_final, 
            format="DD/MM/YYYY"
        )
        
        # Convertir fechas a formato requerido
        self.inicio_str = self.inicio.strftime("%Y%m%d")
        self.final_str = self.final.strftime("%Y%m%d")
        
        # Slider de tolerancia
        self.tolerancia = st.slider(
            "Selecciona el (%) de tolerancia para los :red[días de cobertura]", 
            0, 100, 10
        ) / 100
    
    def cargar_datos_en_paralelo(self, archivos):
        """
        Carga datos de múltiples archivos en paralelo usando ThreadPoolExecutor.
        
        Args:
            archivos (dict): Diccionario con rutas de archivos
        
        Returns:
            dict: Diccionario con DataFrames cargados
        """
        dfs = {}
        with ThreadPoolExecutor() as executor:
            # Cargar archivos MB
            future_to_sheet = {
                executor.submit(pd.read_excel, archivo, sheet_name='Sheet1'): key
                for key, archivo in archivos.items() if key != 'datasets'
            }
            
            # Cargar hojas de datasets
            sheets_future = executor.submit(
                lambda: {
                    sheet: pd.read_excel(archivos['datasets'], sheet_name=sheet) 
                    for sheet in [
                        'db_capacidad_instalada', 
                        'db_cuota', 
                        'db_ratios_planta_insumo', 
                        'db_insumos'
                    ]
                }
            )
            
            # Recolectar resultados
            for future in future_to_sheet:
                key = future_to_sheet[future]
                dfs[key] = future.result()
            
            dfs.update(sheets_future.result())
        
        return dfs
    
    def preprocesar_datos(self, dfs):
        """
        Preprocesa los datos realizando mapeos y generando identificadores.
        
        Args:
            dfs (dict): Diccionario con DataFrames
        
        Returns:
            tuple: DataFrames procesados para análisis
        """
        # Crear mapeo de id_sap a id_insumo
        df_insumos = pd.DataFrame(dfs['db_insumos'])
        mapeo_sap_insumo = df_insumos[['id_sap', 'id_insumo']].drop_duplicates()
        
        # Mapear id_insumo en MB51 y MB52
        for archivo in ['mb51', 'mb52']:
            if 'Material' in dfs[archivo].columns:
                dfs[archivo] = pd.merge(
                    dfs[archivo], 
                    mapeo_sap_insumo.rename(columns={'id_sap': 'Material'}),
                    on='Material', 
                    how='left'
                )
        
        # Generar valores de centros
        df_valor_centros = generar_ids_y_stock_valor(dfs['mb52'], 'general')
        
        # Generar IDs y stocks
        dfs['mb51'] = generar_ids_y_stock(dfs['mb51'])
        df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general = generar_y_separar_mb52(dfs['mb52'])
        
        # Preparar datos adicionales
        df_cuota = pd.DataFrame(dfs['db_cuota'])
        df_ratios = pd.DataFrame(dfs['db_ratios_planta_insumo'])
        df_ratios['id_mix'] = df_ratios['id_localidad'] + df_ratios['id_insumo'].astype(str)
        
        return df_valor_centros, df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general, df_cuota, dfs, df_insumos
    
    def procesar_datos_principales(self, dfs, df_insumos):
        """
        Procesa los datos principales generando métricas de consumo y producción.
        
        Args:
            dfs (dict): Diccionario con DataFrames
            df_insumos (pd.DataFrame): DataFrame de insumos
        
        Returns:
            Tuple de DataFrames procesados
        """
        df_homologado = pd.DataFrame(df_insumos)
        df_homologado['id_mix'] = df_homologado['id_localidad'] + df_homologado['id_insumo'].astype(str)
        
        df_ratios = pd.DataFrame(dfs['db_ratios_planta_insumo'])
        df_ratios['id_mix'] = df_ratios['id_localidad'] + df_ratios['id_insumo'].astype(str)
        
        df_homologacion = pd.merge(
            df_homologado, 
            df_ratios[['id_mix','ratio_nominal','familia','familia_2']],
            on='id_mix', 
            how='left'
        )
        
        df_base = pd.merge(
            df_homologacion, 
            dfs['db_capacidad_instalada'][['id_localidad', 'cip', 'rendimiento', 'cobertura_ideal', 'maxima_descarga', 'cobertura_meta']],
            on='id_localidad', 
            how='left'
        )
        
        # Calcular stock de cobertura ideal
        df_base['stock_cobertura_ideal'] = (
            (df_base['ratio_nominal'] * df_base['maxima_descarga']) / df_base['rendimiento'] * df_base['cobertura_ideal']
        )
        
        # Calcular consumo total
        df_consumo_total = dfs['mb51'].groupby(['id_localidad', 'id_insumo'])['Cantidad'].sum().abs().reset_index()
        
        # Consultar datos de pesca
        df_datos, df_dias_produccion = consultar_pesca(self.inicio_str, self.final_str)
        
        # Agregar días de pesca y consumo diario
        df_consumo_total = pd.merge(
            df_consumo_total, 
            df_dias_produccion[['id_localidad', 'dias_de_pesca']], 
            on='id_localidad', 
            how='left'
        )
        df_consumo_total['consumo_diario'] = df_consumo_total['Cantidad'] / df_consumo_total['dias_de_pesca'].fillna(1)
        df_consumo_total['id_localidad_insumo'] = df_consumo_total['id_localidad'].astype(str) + df_consumo_total['id_insumo'].astype(str)
        
        return df_base, df_consumo_total, df_datos, df_dias_produccion
    
    def ejecutar_analisis(self):
        """
        Ejecuta el análisis completo de gestión de insumos.
        Procesa los datos, genera resultados y permite descarga.
        """
        if st.button("Ejecutar análisis"):
            if all([self.uploaded_file_datasets, self.uploaded_file_mb51, self.uploaded_file_mb52]):
                with st.spinner('Procesando los datos, por favor espera...'):
                    # Cargar archivos
                    archivos_subidos = {
                        'datasets': self.uploaded_file_datasets,
                        'mb51': self.uploaded_file_mb51,
                        'mb52': self.uploaded_file_mb52,
                    }
                    
                    # Cargar datos
                    dfs = self.cargar_datos_en_paralelo(archivos_subidos)
                    
                    # Preprocesar datos
                    (df_valor_centros, df_mb52_produccion, df_mb52_transito, 
                     df_mb52_hub, df_mb52_general, df_cuota, dfs, df_insumos) = self.preprocesar_datos(dfs)
                    
                    # Procesar datos principales
                    df_base, df_consumo_total, df_datos, df_dias_produccion = self.procesar_datos_principales(dfs, df_insumos)
                    
                    # Procesar datos finales
                    df_resultado, df_resultado_por_insumo = procesar_datos(
                        df_base, df_mb52_produccion, df_mb52_transito, 
                        df_mb52_hub, df_mb52_general, 
                        df_consumo_total, dfs['db_insumos']
                    )
                    
                    # id de temporada
                    df_resultado['temporada'] = df_cuota['temporada'].iloc[0]
                    
                    # Obtener la fecha y hora en UTC-5 (Lima, Perú)
                    tz_lima = pytz.timezone("America/Lima")
                    fecha_hora_lima = datetime.now(tz_lima).strftime("%Y-%m-%d %H:%M:%S")
                    df_resultado["fecha_ejecucion"] = fecha_hora_lima
                    
                    # Proyección de pesca
                    df_proyeccion_pesca = realizar_proyeccion(df_datos)
                    
                    # Guardar resultados
                    self.guardar_resultados(
                        df_resultado, df_resultado_por_insumo, 
                        df_datos, df_valor_centros, 
                        df_proyeccion_pesca, df_cuota
                    )
            else:
                st.warning("Por favor, sube todos los archivos requeridos antes de ejecutar el análisis.")
    
    def guardar_resultados(self, *args):
        """
        Guarda los resultados en un archivo Excel.
        
        Args:
            *args: DataFrames a guardar
        """
        with pd.ExcelWriter('resultados.xlsx') as writer:
            nombres_hojas = [
                'seguimiento_insumos', 
                'seguimiento_por_insumo', 
                'seguimiento_pesca', 
                'valorizado_centros', 
                'proyeccion_pesca', 
                'cuota'
            ]
            
            for df, nombre in zip(args, nombres_hojas):
                df.to_excel(writer, sheet_name=nombre, index=False)
            
            # Hoja de parámetros
            pd.DataFrame({'tolerancia': [self.tolerancia]}).to_excel(writer, sheet_name='parametros', index=False)
        
        # Botón de descarga
        with open("resultados.xlsx", "rb") as file:
            st.download_button(
                label="Descargar resultados en Excel", 
                data=file, 
                file_name="resultados.xlsx"
            )
    
    def main(self):
        """Método principal que ejecuta la aplicación."""
        self.ejecutar_analisis()

# Inicialización de la aplicación
def main():
    app = GestionInsumos()
    app.main()

if __name__ == "__main__":
    main()