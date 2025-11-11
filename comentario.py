import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    print(event)

    # Entrada (json)
    body = event.get('body')
    if isinstance(body, str):
        import json
        body = json.loads(body)

    tenant_id = body['tenant_id']
    texto = body['texto']

    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_name = os.environ["INGESTA_BUCKET"]

    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        },
        'timestamp': datetime.utcnow().isoformat()
    }

    # 1️⃣ Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response_db = table.put_item(Item=comentario)

    # 2️⃣ Guardar también en S3 (Estrategia “Ingesta Push”)
    s3 = boto3.client('s3')
    file_key = f"comentarios/{tenant_id}/{uuidv1}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(comentario, indent=2),
        ContentType='application/json'
    )

    print(f"Comentario guardado en DynamoDB y en S3: {file_key}")

    # Salida (json)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'comentario': comentario,
            'dynamodb_response': response_db,
            's3_file': file_key
        })
    }
