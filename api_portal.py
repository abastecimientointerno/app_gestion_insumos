import requests
import json
import pandas as pd

def consultar_pesca(inicio, final):
    # URL de la API
    url = "https://node-flota-prd.cfapps.us10.hana.ondemand.com/api/reportePesca/ConsultarPescaDescargada"

    # Encabezados de la solicitud
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "es-ES,es;q=0.9",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://tasaproduccion.launchpad.cfapps.us10.hana.ondemand.com",
    }

    # Datos de la solicitud (payload)
    payload = {
        "p_options": [],
        "options": [
            {
                "cantidad": "10",
                "control": "MULTIINPUT",
                "key": "FECCONMOV",
                "valueHigh": final,
                "valueLow": inicio
            }
        ],
        "p_rows": "",
        "p_user": "JHUAMANCIZA"
    }

    # Realizar la solicitud POST
    response = requests.post(url, headers=headers, json=payload)

    # Verificar la respuesta
    if response.status_code == 200:
        # Convertir la respuesta JSON a un diccionario de Python
        response_dict = response.json()

        # Crear un DataFrame a partir de los datos de "str_des"
        df_datos = pd.DataFrame(response_dict['str_des'])

        # Convertir la columna "FCSAZ" a tipo datetime para trabajar con fechas
        df_datos['FCSAZ'] = pd.to_datetime(df_datos['FCSAZ'], format='%d/%m/%Y')

        # Eliminar duplicados para contar solo días únicos de pesca por planta
        unique_days_per_plant = df_datos[['WERKS', 'FCSAZ']].drop_duplicates()

        # Contar los días únicos de pesca por cada planta
        df_dias_produccion = unique_days_per_plant['WERKS'].value_counts().reset_index()
        df_dias_produccion.columns = ['id_localidad', 'dias_de_pesca']

        return df_datos, df_dias_produccion

    else:
        print(f"Error: {response.status_code}")
        return None, None
