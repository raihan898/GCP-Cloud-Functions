import os
import pandas as pd
from google.cloud import storage
from flask import Request, Response
import io

# Initialize Google Cloud Storage client
storage_client = storage.Client()

# Specify your source and destination bucket names, and the input and output file names
source_bucket_name = "new-bucket0001"
destination_bucket_name = "new-bucket-203"  # Change to the new bucket name
input_csv_file = "Orders.csv"
output_parquet_file = "output_output_22.parquet"  # Change the output file name

def convert_csv_to_parquet(request: Request):
    try:
        # Get the data from the CSV file in the source Google Cloud Storage bucket
        source_bucket = storage_client.get_bucket(source_bucket_name)
        source_blob = source_bucket.blob(input_csv_file)

        # Download the CSV data as a string
        csv_data = source_blob.download_as_text()

        # Read the data into a DataFrame
        data = pd.read_csv(io.StringIO(csv_data))

        # Convert CSV to Parquet
        parquet_data_io = io.BytesIO()
        data.to_parquet(parquet_data_io, index=False)

        # Upload the Parquet data to the destination Google Cloud Storage bucket
        destination_bucket = storage_client.get_bucket(destination_bucket_name)
        parquet_blob = destination_bucket.blob(output_parquet_file)
        parquet_blob.upload_from_string(parquet_data_io.getvalue())

        return Response("CSV to Parquet conversion complete. Parquet file uploaded to the destination bucket.", status=200)

    except Exception as e:
        return Response("Error: " + str(e), status=500)

# This is the HTTP trigger for your function
def http_trigger(request: Request):
    if request.method == "POST":
        return convert_csv_to_parquet(request)  # Call the Parquet conversion function
    else:
        return Response("Invalid request method. Use POST.", status=400)

# The entry point for your Cloud Function
def main(request):
    return http_trigger(request)
