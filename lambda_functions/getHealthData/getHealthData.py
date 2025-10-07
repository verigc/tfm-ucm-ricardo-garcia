
import requests
import pandas as pd
import time
import boto3
from botocore.exceptions import ClientError
import awswrangler as wr
import json
import sys
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


def make_api_request(url, params=None, max_retries=5, initial_delay=1):
    """
    Realiza una solicitud a la API con manejo de reintentos y límites de tasa.
    """
    headers = {'accept': 'application/json',
               'content-type': 'application/json'}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 410:
                print(
                    f"Error 410 Gone: Versión de la API retirada. usar /v3/. Detalles: {response.text}")[2]
                return None
            elif response.status_code == 429:
                reset_time = int(response.headers.get('x-ratelimit-reset', 60))
                print(
                    f"""Error 429 Too Many Requests: Límite de tasa excedido.
                    Esperando {reset_time} segundos antes de reintentar.""")[1]
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
            mode="overwrite_partitions",
            compression="snappy",
            filename_prefix=prefix
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

    CLAVE_API = get_secret('tfm-ucm-dev').get('inclasns')
    bucket_name = os.getenv('bucket_name')
    print(f'Bucket: {bucket_name}')
    
    # declaramos el endpoint para la lista de variables
    base_api_uri = f'https://inclasns.sanidad.gob.es'
    comp_uri  = f'/api/v2/indicador?API_KEY={CLAVE_API}'
    api_uri = base_api_uri + comp_uri

    # obtenemos la lista de indicadores

    try:
        response = requests.get(api_uri)
        if response.status_code == 200:
            print('Indicadores obtenidos')
            df_indicadores = pd.DataFrame(response.json())

            # Ahora preguntamos por los datos de cada codigo, solo de aquellos que nos interesan
            indicadores = list(df_indicadores[df_indicadores['nombre'].str.contains('EPOC|asma|tosferina', regex=True)]['codigo'])
            print(f"Obteniendo datos para siguientes codigos de indicadores: {indicadores}")
            datos_dict = {}
            for indicador in indicadores:
                print(f"Indicador: {indicador}")
                comp_uri = f'/api/v2/datos?indicador={indicador}&sexo=&ccaa=&anio=&API_KEY={CLAVE_API}'
                # comp_uri = f'/api/v2/datos?indicador={indicador}&anio=2023&anio=2024&API_KEY={CLAVE_API}'
                api_uri = base_api_uri + comp_uri
                response = requests.get(api_uri)
                if response.status_code == 200:
                    datos_dict[f'{indicador}'] = response.json()
                else:
                    print(response.text)
                    continue

            # los convertimos a df para guardarlos como parquet
            datos = {}
            for df in list(datos_dict.keys()):
                tmp_df = pd.DataFrame(datos_dict[df][0]['datos'])
                tmp_df['codigo'] = datos_dict[df][0]['codigo']
                tmp_df =  tmp_df.merge(df_indicadores[['nombre', 'codigo']], how='left')
                datos[df] = tmp_df

            # guadamos en s3 cada df obtenido
            for df in datos:
                file_name = datos[df]['nombre'].unique()[0].replace(' ', '_').replace('.', '').replace(',', '')
                s3_path = f"{bucket_name}/staging/inclasns/{file_name}.parquet"
                print(f"Guardando : {s3_path}")
                put_s3_object(
                    s3_path=s3_path,
                    df=datos[df].astype('str')
                    # partition_cols=['year', 'month', 'day'],
                    # prefix=f'measure_sensor_{sensor_id}' # Prefijo único por si acaso
                )

            return {
                'statusCode': 200,
                'body': json.dumps(f'Datos guardados en: {bucket_name}'),
                'size': len(datos)
            }
            
        else:
            print(response.text)
    except Exception as e:
        print(e)

    



