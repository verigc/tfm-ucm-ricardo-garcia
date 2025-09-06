import requests
import json
import pandas as pd
import time
from datetime import datetime, timezone
import io
import awswrangler as wr
import boto3
import os

headers = {'X-API-Key': '71c0183ac47f04eb410422bb29540a3b1dc165c2895336c2973d40d280fa9253',
 'accept': 'application/json',
 'content-type': 'application/json'}


 
def make_api_request(url, params, max_retries=5, initial_delay=1):
    """
    Realiza una solicitud a la API con manejo de reintentos y límites de tasa.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers)

            # Imprimir cabeceras de límite de tasa para depuración (opcional)
            # print(f"Headers: x-ratelimit-remaining={response.headers.get('x-ratelimit-remaining')}, x-ratelimit-reset={response.headers.get('x-ratelimit-reset')}")

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 410:
                print(
                    f"Error 410 Gone: Versión de la API retirada. usar /v3/. Detalles: {response.text}")[2]
                return None
            elif response.status_code == 429:
                reset_time = int(response.headers.get('x-ratelimit-reset', 60))
                print(
                    f"Error 429 Too Many Requests: Límite de tasa excedido. \
                    Esperando {reset_time} segundos antes de reintentar.")
                time.sleep(reset_time + 1)  # Esperar el tiempo de reseteo + 1 segundo
            else:
                print(
                    f"Error al descargar datos (Intento {attempt + 1}/{max_retries}): {response.status_code} - {response.text}")
                time.sleep(initial_delay * (2 ** attempt))  # Retroceso exponencial [3, 4]
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión (Intento {attempt + 1}/{max_retries}): {e}")
            time.sleep(initial_delay * (2 ** attempt))  # Retroceso exponencial
    print(f"Fallo después de {max_retries} reintentos para URL: {url} con params: {params}")
    return None


#============= Parametros de medicion ============


def get_parameters():
    parameters_base_url = 'https://api.openaq.org/v3/parameters'
    params = {"limit": 1000}
    data = make_api_request(parameters_base_url, params)
    if data and 'results' in data:
        return data



def get_locations_in_country(country_code="ES", limit=1000):
    """
    Obtiene todas las ubicaciones (y sus sensores) para un país dado, manejando la paginación.
    """
    locations_base_url = "https://api.openaq.org/v3/locations"
    all_locations = []
    current_page = 1
    total_pages = 1
    page_limit = limit


    print(f"Obteniendo ubicaciones para {country_code}...")
    while current_page <= total_pages:

        # params = {"iso": country_code, "limit": limit,"page": current_page}
        params = {"iso": "ES", "limit": limit}

        data = make_api_request(locations_base_url, params)

        if data and 'results' in data:
            locations_page = data['results']
            all_locations.extend(locations_page)

            total_results = data['meta'].get('found', 0)
            if total_results == '>1000':
                total_pages +=1
            else:
                total_pages = (total_results + page_limit - 1) // page_limit

            page_limit = data['meta'].get('limit', limit)
            print(f"Total de ubicaciones encontradas: {total_results}. Total de páginas estimadas: {total_pages}")

            current_page += 1
            time.sleep(0.5)  # Pequeña pausa entre solicitudes de página [1]
        else:
            print(
                f"No se obtuvieron datos de ubicaciones en la página {current_page}. Terminando la búsqueda de ubicaciones.")
            break  # Salir si no hay datos o hay un error irrecuperable

    return all_locations



def put_s3_object(s3_path, df, partition_cols=None, prefix=None):
    """
    Esta funcion recibe un dataframe y lo carga en s3 particionado
    por las columnas indicadas en partition_list
    :param
        s3_path: path s3 donde se va a cargar
        df: Dataframe
        partition_cols: lista de columnas por las cuales particionar
    :return:
    """

    if partition_cols is not None:
        resultado = wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,  # ¡Esta es la clave para habilitar el particionamiento!
            partition_cols=partition_cols,
            mode= "append", # "overwrite_partitions"
            compression="snappy",
            filename_prefix=prefix
        )
    else:
        resultado = wr.s3.to_parquet(
            df=df
            , path=s3_path
            # , mode='overwrite'
        )

    print(f"Resultado de carga en S3: {resultado}")

    return resultado


# --- Función Handler para AWS Lambda ---
def lambda_handler(event, context):
    print("Iniciando ejecución de la función Lambda para descargar lista de sensores disponibles en España...")

    # Obtenemos las variables de evento
    bucket_name = os.getenv('bucket_name')
    country_code = os.getenv('country_code', 'ES')
    start_date = os.getenv('start_date', "2024-01-01T00:00:00Z")
    end_date = os.getenv('end_date', datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z'))

    # ======================== Parameters & Locations (Sin cambios) ========================
    # Esta parte generalmente no consume mucha memoria, la dejamos como está.

    # ... (el código para obtener y cargar 'parameters' se queda igual) ...
    parameters_df = pd.DataFrame(get_parameters()['results'])
    put_s3_object(s3_path=f'{bucket_name}/staging/OpenAQ/parameters/parameters.parquet', df=parameters_df)


    # ... (el código para obtener y cargar 'locations' se queda igual) ...
    spain_locations = get_locations_in_country(country_code=country_code)
    spain_locations_df = pd.DataFrame(spain_locations)
    # ... (todas tus transformaciones de spain_locations_df se quedan igual) ...
    spain_locations_df['country'] = spain_locations_df['country'].apply(lambda x: x.get('name'))
    spain_locations_df['latitud'] = spain_locations_df['coordinates'].apply(lambda x: x.get('latitude'))
    spain_locations_df['longitud'] = spain_locations_df['coordinates'].apply(lambda x: x.get('longitude'))
    spain_locations_df['datetimeFirst'] = spain_locations_df['datetimeFirst'].apply(
        lambda x: x.get('local') if x else None)
    spain_locations_df['datetimeLast'] = spain_locations_df['datetimeLast'].apply(
        lambda x: x.get('local') if x else None)
    spain_locations_df = spain_locations_df.explode('sensors')
    spain_locations_df['sensor_id'] = spain_locations_df['sensors'].apply(lambda x: x.get('id', None))
    spain_locations_df['parameter_id'] = spain_locations_df['sensors'].apply(
        lambda x: x.get('parameter', {}).get('id', None))
    spain_locations_df.drop(
        columns=['owner', 'provider', 'isMobile', 'instruments', 'sensors', 'licenses', 'distance',
                 'coordinates'], inplace=True)
    put_s3_object(f'{bucket_name}/staging/OpenAQ/locations/locations.parquet', df=spain_locations_df)
    
    sensor_ids_to_fetch = set(spain_locations_df.sensor_id)
    sensor_ids_to_fetch = [str(sensor_id) for sensor_id in sensor_ids_to_fetch]
    print(f"Se encontraron {len(sensor_ids_to_fetch)} IDs de sensores únicos.")

    return sensor_ids_to_fetch
