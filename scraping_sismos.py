import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    response = requests.get(url)

    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'No se pudo acceder al sitio del IGP.'
        }

    soup = BeautifulSoup(response.content, 'html.parser')

    # Buscar la tabla HTML de sismos
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de sismos.'
        }

    # Obtener encabezados
    headers = [header.text.strip() for header in table.find_all('th')]

    # Obtener las primeras 10 filas
    rows_html = table.find_all('tr')[1:11]
    rows = []
    for row in rows_html:
        cols = row.find_all('td')
        if len(cols) >= 4:
            item = {
                'id': str(uuid.uuid4()),
                'reporte': cols[0].get_text(strip=True),
                'referencia': cols[1].get_text(strip=True),
                'fecha_hora': cols[2].get_text(strip=True),
                'magnitud': cols[3].get_text(strip=True)
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
        'body': f"{len(rows)} sismos insertados correctamente."
    }
