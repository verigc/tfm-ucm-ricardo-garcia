

import requests
import json
import pandas as pd
import time
from datetime import datetime, timezone
import io
import awswrangler as wr
import boto3
from botocore.exceptions import ClientError
import os 



def get_secret(secret_name):

    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)

    return secret


def make_api_request(url, params, max_retries=5, initial_delay=1):
    """
    Realiza una solicitud a la API con manejo de reintentos y límites de tasa.
    """
    CLAVE_API = get_secret('tfm-ucm').get('openaq')
    headers = {'X-API-Key': CLAVE_API,
        'accept': 'application/json',
        'content-type': 'application/json'}
    
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



def get_daily_measurements_for_sensor(sensor_id, datetime_from=None, datetime_to=None, limit=1000):
    """
    Descarga mediciones diarias agregadas para un sensor específico,
    manejando la paginación para obtener todos los datos dentro del rango.
    """
    daily_measurements_base_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/daily"
    all_daily_measurements = []
    current_page = 1
    total_pages = 1
    page_limit = limit

    while current_page <= total_pages:
        params = {
            "limit": limit,
            "page": current_page
        }
        # Usar 'datetime_from' y 'datetime_to' para endpoints agregados
        if datetime_from:
            params["datetime_from"] = datetime_from
        if datetime_to:
            params["datetime_to"] = datetime_to

        data = make_api_request(daily_measurements_base_url, params)

        if data and 'results' in data:
            daily_measurements_page = data['results']
            all_daily_measurements.extend(daily_measurements_page)

            # agregamos el id al diccionario
            for i,j in enumerate(all_daily_measurements):
                all_daily_measurements[i]['sensor_id'] = sensor_id

            total_results = data['meta'].get('found', 0)
            if total_results == '>1000':
                total_pages +=1
            else:
                total_pages = (total_results + page_limit - 1) // page_limit

            page_limit = data['meta'].get('limit', limit)
            print(
                f"  Sensor {sensor_id}: Total de mediciones diarias encontradas: {total_results}. \
                Total de páginas estimadas: {total_pages}")

            current_page += 1
            time.sleep(0.1)  # Pequeña pausa entre solicitudes de página [1]
        else:
            print(
                f"  No se obtuvieron datos diarios para el sensor {sensor_id} en la página {current_page}.\
                 Terminando la descarga para este sensor.")
            break

    return all_daily_measurements


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
            # database="tfm-ucm",
            # table="measurments"
        )
    else:
        resultado = wr.s3.to_parquet(
            df=df
            , path=s3_path
            # , mode='overwrite'
        )

    return resultado

# --- Función Handler para AWS Lambda ---
def lambda_handler(event, context):
    print("Iniciando ejecución de la función Lambda para descargar datos de calidad del aire...")

    # Obtenemos las variables de evento
    bucket_name = os.getenv('bucket_name')
    country_code = os.getenv('country_code', 'ES')
    start_date = os.getenv('start_date', "2024-01-01T00:00:00Z")
    end_date = os.getenv('end_date', datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z'))
    sensor_id = event

    # ======================== Measurements (Lógica Optimizada) ========================
    
    total_mediciones_cargadas = 0

    # CAMBIO CLAVE: Ya no necesitamos la lista `all_spain_daily_measurements`. La eliminamos.
    
    # for i, sensor_id in enumerate(list(sensors)):
    # print(f"[{i + 1}/{len(sensors)}] Procesando sensor ID: {sensor_id} (Rango: {start_date} a {end_date})...")
    print(f"Procesando sensor ID: {sensor_id} (Rango: {start_date} a {end_date})...")
    
    daily_measurements_for_sensor = get_daily_measurements_for_sensor(
        sensor_id=sensor_id,
        datetime_from=start_date,
        datetime_to=end_date,
        limit=1000
    )
    
    # CAMBIO CLAVE: Si no hay datos para este sensor, simplemente continuamos al siguiente.
    if not daily_measurements_for_sensor:
        print(f"  Sensor {sensor_id}: No se encontraron mediciones. Saltando.")
        # pass # Pasa a la siguiente iteración del bucle
    
    # CAMBIO CLAVE: Creamos un DataFrame PEQUEÑO, solo para el sensor actual.
    else:
        sensor_df = pd.DataFrame(daily_measurements_for_sensor)
        # CAMBIO CLAVE: Aplicamos TODAS las transformaciones a este pequeño DataFrame.
        # Esta es la misma lógica que tenías al final, pero aplicada a `sensor_df`.
        sensor_df['datetimeFrom'] = pd.to_datetime(sensor_df['period'].apply(
            lambda x: x.get('datetimeFrom', {}).get('local')), errors='coerce')
        sensor_df['datetimeTo'] = pd.to_datetime(sensor_df['period'].apply(
            lambda x: x.get('datetimeTo', {}).get('local')), errors='coerce')
        
        sensor_df['datetimeFrom'] = sensor_df['datetimeFrom'].apply(lambda x: x.tz_convert(None) if pd.notnull(x) else x)
        sensor_df['datetimeTo'] = sensor_df['datetimeTo'].apply(lambda x: x.tz_convert(None) if pd.notnull(x) else x)

        # Manejar el caso de que la columna summary no exista o esté vacía
        if 'summary' in sensor_df.columns:
            normalized_summary = pd.json_normalize(sensor_df['summary'])
            sensor_df = pd.concat([sensor_df.reset_index(drop=True), normalized_summary], axis=1)

        sensor_df.drop(columns=['flagInfo', 'parameter', 'period', 'coordinates', 'summary', 'coverage'],
                        inplace=True, errors='ignore') # 'errors=ignore' por si alguna columna no existe

        # Creamos las columnas de particion
        # sensor_df['year'] = sensor_df['datetimeFrom'].dt.year
        # sensor_df['month'] = sensor_df['datetimeFrom'].dt.month
        # sensor_df['day'] = sensor_df['datetimeFrom'].dt.day
        
        # CAMBIO CLAVE: Cargamos el DataFrame de este único sensor a S3.
        # `awswrangler` añadirá los archivos a las particiones correctas sin sobreescribir
        # los datos de otros sensores que caigan en la misma partición.
        print(f"  Sensor {sensor_id}: Encontradas {len(sensor_df)} mediciones. Cargando a S3...")
        s3_path = f"s3://{bucket_name}/staging/OpenAQ/measurements/{sensor_id}.parquet"
        put_s3_object(
            s3_path= s3_path,
            df=sensor_df
            # partition_cols=['year', 'month', 'day'],
            # prefix=f'measure_sensor_{sensor_id}' # Prefijo único por si acaso
        )
        
        total_mediciones_cargadas += len(sensor_df)
        print(f"  Sensor {sensor_id}: Carga completa.")
        
        # Al final de esta iteración, `sensor_df` es liberado de la memoria.
        # time.sleep(0.1)

        print(f"\nProceso completado. Total de mediciones diarias cargadas en S3: {total_mediciones_cargadas}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Datos guardados en S3: s3://{bucket_name}'),
            'size': len(sensor_df)
        }