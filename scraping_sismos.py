import requests
import boto3
import uuid

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/api/sismo"
    response = requests.get(url)

    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'No se pudo acceder a la API del IGP.'
        }

    data = response.json()
    rows = []

    for sismo in data[:10]:  # Solo los 10 más recientes
        item = {
            'id': str(uuid.uuid4()),
            'reporte': sismo.get('title', ''),
            'referencia': sismo.get('reference', ''),
            'fecha_hora': sismo.get('datetime', ''),
            'magnitud': sismo.get('mag', '')
        }
        rows.append(item)

    # Conectar a DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaSismosIGP')

    # Eliminar items antiguos
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get('Items', []):
            batch.delete_item(Key={'id': item['id']})

    # Insertar nuevos items
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': f"{len(rows)} sismos insertados correctamente desde API."
    }
