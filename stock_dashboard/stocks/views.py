import boto3
import os
import pandas as pd
import io
from datetime import datetime
from django.conf import settings
from django.shortcuts import render

def stock_data_view(request):
    s3 = boto3.client(
        's3',
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        region_name=settings.AWS_REGION
    )

    response = s3.list_objects_v2(Bucket=settings.BUCKET_NAME, Prefix='stock-data/')
    files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)

    if not files:
        return render(request, "stocks/stocks_data.html", {"data": None})

    # Load the latest file
    obj = s3.get_object(Bucket=settings.BUCKET_NAME, Key=files[0]["Key"])
    buffer = io.BytesIO(obj['Body'].read())
    df = pd.read_parquet(buffer)

    # Convert ts_recv to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df["ts_recv"]):
        df["ts_recv"] = pd.to_datetime(df["ts_recv"])

    # Handle date filtering
    selected_date = request.GET.get("date")
    if selected_date:
        try:
            selected = pd.to_datetime(selected_date).date()
            df = df[df["ts_recv"].dt.date == selected]
        except Exception as e:
            print(f"Invalid date filter: {e}")

    data = df.to_dict(orient='records')
    columns = df.columns.tolist()

    return render(request, "stocks/stocks_data.html", {
        "data": data,
        "columns": columns,
        "selected_date": selected_date or ""
    })


