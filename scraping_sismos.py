import requests
import boto3
import uuid
###### no funciona porque .......
def lambda_handler(event, context):
    datos = obtener_ultimo_sismo()
    if not datos:
        return {"statusCode": 404,"body": "No se obtuvieron datos del IGP."}

    rows = []
    for feat in datos[:10]:
        a = feat["attributes"]
        rows.append({
            "id": str(uuid.uuid4()),
            "fecha_hora": a["FECHA_HORA"],
            "magnitud": a["MAGNITUD"],
            "referencia": a.get("UBICACION", "")
        })

    db = boto3.resource('dynamodb').Table('TablaSismosIGP')
    with db.batch_writer() as batch:
        for r in rows:
            batch.put_item(Item=r)

    return {"statusCode":200,
            "body": f"{len(rows)} sismos insertados correctamente."}

def obtener_ultimo_sismo():
    url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/UltimoSismo/MapServer/0/query"
    params = {"where":"1=1","outFields":"*","f":"json",
              "orderByFields":"FECHA_HORA DESC","resultRecordCount":10}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("features", [])

