import requests

def lambda_handler(event=None, context=None):
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return {
            "statusCode": 200,
            "body": data  # Devuelve TODO el contenido JSON tal cual viene
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }

