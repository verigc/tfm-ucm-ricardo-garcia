import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import awswrangler as wr
import boto3
import json
import os

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

def lambda_handler(event, context):

    headers = {'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Host': 'apidatos.ree.es'}

    fecha_ini = pd.to_datetime(os.getenv('fecha_ini'))
    fecha_fin = pd.to_datetime(os.getenv('fecha_fin'))
    time_trunc = os.getenv('time_trunc')
    base_uri = 'https://apidatos.ree.es'
    uri_definition = os.getenv('uri_definition')
    bucket_name = os.getenv('bucket_name')

    api_uri = f'{base_uri}{uri_definition}?\
    start_date={fecha_ini}&\
    end_date={fecha_fin}&\
    time_trunc={time_trunc}'

    # Calculamos los dias entre periodos para no exceder el lmite de la API de 1 año
    bloques = []
    if (fecha_fin - fecha_ini).days > 365:
        actual_inicio = fecha_ini
        while actual_inicio < fecha_fin:
            # Último día del año actual
            actual_fin = datetime(actual_inicio.year, 12, 31, 23, 59, 59)
            # Si el último día del año es mayor que fecha_fin, usamos fecha_fin
            if actual_fin > fecha_fin:
                actual_fin = fecha_fin
            bloques.append((actual_inicio, actual_fin))
            # Siguiente bloque: primer día del siguiente año
            actual_inicio = datetime(actual_inicio.year + 1, 1, 1)
    else:
        bloques.append((fecha_ini, fecha_fin))

    all_data = {}
    for inicio, fin in bloques:
        print(f"Inicio: {inicio.strftime('%Y-%m-%d')}, Fin: {fin.strftime('%Y-%m-%d')}")
        api_uri = f'{base_uri}{uri_definition}?\
            start_date={inicio}&\
            end_date={fin}&\
            time_trunc={time_trunc}'
        
        response = requests.get(api_uri, headers= headers)
        if response.status_code == 200:
            data = response.json()
            if len(all_data)==0:
                all_data.update(data)
            else:
                all_data = data.copy()
        else:
            print(f"Error: {response.status_code} - {response.text}")
            continue

    df = pd.DataFrame([x for x in all_data['included'][0]['attributes']['values'] if x['value'] is not None])
    s3_path = f"s3://{bucket_name}/staging/ree/consumo_energetico_2024_2025.parquet"
    put_s3_object(s3_path
                  , df
                #   , partition_cols=['year', 'month', 'day']
                #   , prefix='measure'
                  )


    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(f'Datos guardados en: {bucket_name}, df shape: {df.shape}')
    }
