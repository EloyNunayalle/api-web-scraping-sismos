import requests
import boto3
import uuid
from datetime import datetime

def lambda_handler(event, context):
    # API oficial IGP
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"

    resp = requests.get(url)
    if resp.status_code != 200:
        return {'statusCode': resp.status_code, 'body': 'Error al obtener datos desde API IGP'}

    try:
        data = resp.json()
    except ValueError:
        return {'statusCode': 500, 'body': 'Respuesta no es JSON'}

    # Parsear fechas y ordenar de más reciente a más antigua
    for item in data:
        fecha = item.get('fecha_hora_local')
        try:
            # Ajusta formato si es necesario
            item['_ts'] = datetime.strptime(fecha, "%d/%m/%Y %H:%M:%S")
        except Exception:
            item['_ts'] = datetime.min

    data_sorted = sorted(data, key=lambda x: x['_ts'], reverse=True)
    top10 = data_sorted[:10]

    # Preparar objetos para insertar
    sismos = []
    for item in top10:
        sismos.append({
            'id': str(uuid.uuid4()),
            'reporte': item.get('reporte', ''),
            'referencia': item.get('referencia', ''),
            'fecha_hora': item.get('fecha_hora_local', ''),
            'magnitud': str(item.get('magnitud', ''))
        })

    # Conexión a DynamoDB
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('TablaSismosIGP')

    # Limpiar la tabla antes de insertar nuevos registros
    resp_scan = table.scan()
    with table.batch_writer() as batch:
        for old in resp_scan.get('Items', []):
            batch.delete_item(Key={'id': old['id']})

    # Insertar los 10 más recientes
    with table.batch_writer() as batch:
        for s in sismos:
            batch.put_item(Item=s)

    return {
        'statusCode': 200,
        'body': {
            'mensaje': f"{len(sismos)} sismos recientes insertados.",
            'sismos': sismos
        }
    }
