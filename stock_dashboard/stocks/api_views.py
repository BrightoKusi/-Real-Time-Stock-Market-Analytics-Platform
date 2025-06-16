from rest_framework.decorators import api_view
from rest_framework.response import Response
import boto3
import pandas as pd
import io
from django.conf import settings

@api_view(['GET'])
def stock_data_api(request):
    s3 = boto3.client(
        's3',
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        region_name=settings.AWS_REGION
    )

    response = s3.list_objects_v2(Bucket=settings.BUCKET_NAME, Prefix='stock-data/')
    files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)

    if not files:
        return Response([])

    obj = s3.get_object(Bucket=settings.BUCKET_NAME, Key=files[0]["Key"])
    buffer = io.BytesIO(obj['Body'].read())
    df = pd.read_parquet(buffer)
    return Response(df.to_dict(orient='records'))
