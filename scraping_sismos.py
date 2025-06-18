import requests
import boto3
import uuid
import json


def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"
    
    response = requests.get(url)
    
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'No se pudo acceder a la API del IGP.'
        }

    try:
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except ValueError:
        return {
            'statusCode': 500,
            'body': 'La respuesta no es un JSON válido.'
        }

    # Tomar los primeros 10 sismos
    sismos = []
    for item in data[:10]:
        sismos.append({
            'id': str(uuid.uuid4()),
            'reporte': item.get('reporte', ''),
            'referencia': item.get('referencia', ''),
            'fecha_hora': item.get('fecha_hora_local', ''),
            'magnitud': item.get('magnitud', '')
        })

    # Conectar a DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaSismosIGP')

    # Eliminar items antiguos
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get('Items', []):
            batch.delete_item(Key={'id': item['id']})

    # Insertar nuevos sismos
    with table.batch_writer() as batch:
        for sismo in sismos:
            batch.put_item(Item=sismo)

    return {
        'statusCode': 200,
        'body': {
            'mensaje': f"{len(sismos)} sismos insertados correctamente desde API JSON.",
            'sismos': sismos
        }
    }
