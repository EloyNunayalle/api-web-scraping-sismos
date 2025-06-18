import requests
import uuid
import boto3

def lambda_handler(event=None, context=None):
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Filtrar los que están publicados
        data = [item for item in data if item.get("publicado") == "1"]

        # Ordenar por código (descendente)
        data.sort(key=lambda x: x.get("codigo", ""), reverse=True)

        # Tomar los 10 más recientes
        top_10 = data[:10]

        # Construir objetos formateados
        sismos = []
        for item in top_10:
            codigo = item.get("codigo", "")
            referencia = item.get("referencia", "")
            fecha = item.get("fecha_local", "")[:10]
            hora = item.get("hora_local", "")[11:19]
            magnitud = item.get("magnitud", "")

            sismos.append({
                "id": str(uuid.uuid4()),
                "reporte": f"IGP/CENSIS/RS {codigo}",
                "referencia": referencia,
                "fecha_hora": f"{fecha} {hora}",
                "magnitud": magnitud
            })

        # Guardar en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaSismosIGP')

        # Eliminar registros anteriores
        scan = table.scan()
        with table.batch_writer() as batch:
            for item in scan.get('Items', []):
                batch.delete_item(Key={'id': item['id']})

        # Insertar los nuevos 10
        with table.batch_writer() as batch:
            for sismo in sismos:
                batch.put_item(Item=sismo)

        return {
            "statusCode": 200,
            "body": {
                "mensaje": f"{len(sismos)} sismos recientes insertados.",
                "sismos": sismos
            }
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }
