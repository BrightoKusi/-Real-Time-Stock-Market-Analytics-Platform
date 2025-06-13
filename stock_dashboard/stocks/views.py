# views.py
import pandas as pd
from io import BytesIO
from django.shortcuts import render
from django.conf import settings
import boto3
import os

def stock_data_view(request):
    # Get optional date filters from query params
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    s3 = boto3.client(
        's3',
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        region_name=settings.AWS_REGION
    )
    bucket = settings.BUCKET_NAME

    # List objects
    response = s3.list_objects_v2(Bucket=bucket, Prefix="stock-data/")
    dfs = []

    for obj in response.get("Contents", []):
        file_obj = s3.get_object(Bucket=bucket, Key=obj["Key"])
        df = pd.read_parquet(BytesIO(file_obj["Body"].read()))
        dfs.append(df)

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)

        # Convert ts_event to datetime if needed
        full_df["ts_event"] = pd.to_datetime(full_df["ts_event"])

        # Apply filters
        if start_date:
            full_df = full_df[full_df["ts_event"] >= start_date]
        if end_date:
            full_df = full_df[full_df["ts_event"] <= end_date]

        # Convert to list of dicts
        data = full_df.to_dict(orient="records")
    else:
        data = []

    return render(request, "stocks/stocks_data.html", {"data": data, "start_date": start_date, "end_date": end_date})
