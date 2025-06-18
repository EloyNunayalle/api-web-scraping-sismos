import requests
import boto3
import uuid
from bs4 import BeautifulSoup
from datetime import datetime

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"
    resp = requests.get(url)
    if resp.status_code != 200:
        return {'statusCode': resp.status_code, 'body': 'No se pudo acceder al endpoint HTML'}

    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.select('tbody tr')
    parsed = []
    for tr in rows:
        cols = tr.find_all('td')
        if len(cols) < 5: continue
        reporte = cols[0].get_text(" ", strip=True)
        referencia = cols[1].text.strip()
        fecha_hora = cols[2].text.strip()
        magnitud = cols[3].text.strip()
        # enlace del reporte sísmico
        enlace = cols[4].find('a')
        descarga = enlace['href'] if enlace else ''
        # parsear fecha
        try:
            ts = datetime.strptime(fecha_hora, "%d/%m/%Y %H:%M:%S")
        except:
            ts = datetime.min
        parsed.append({
            'id': str(uuid.uuid4()),
            'reporte': reporte,
            'referencia': referencia,
            'fecha_hora': fecha_hora,
            'magnitud': magnitud,
            'descarga': descarga,
            '_ts': ts
        })

    # ordenar por fecha descendente y tomar top 10
    top10 = sorted(parsed, key=lambda x: x['_ts'], reverse=True)[:10]

    # Conectar a DynamoDB
    table = boto3.resource('dynamodb').Table('TablaSismosIGP')
    # Eliminar items antiguos
    scan = table.scan()
    with table.batch_writer() as batch:
        for old in scan.get('Items', []):
            batch.delete_item(Key={'id': old['id']})

    # insertar los 10 nuevos
    with table.batch_writer() as batch:
        for s in top10:
            item = {k: v for k, v in s.items() if not k.startswith('_')}
            batch.put_item(Item=item)

    # responder
    return {
        'statusCode': 200,
        'body': {
            'mensaje': f"{len(top10)} sismos recientes insertados.",
            'sismos': top10
        }
    }
