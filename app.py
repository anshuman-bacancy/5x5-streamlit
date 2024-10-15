#!/usr/bin/python3
import streamlit as st
import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO


# streamlit config 
st.set_page_config(page_title="Display matched 5x5 data", layout="wide")


# Initialize boto3 client to interact with S3
s3_client = boto3.client('s3')

# Function to list all Parquet files in a given S3 bucket path (prefix)
def list_parquet_files(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        # Return a list of file keys for the Parquet files
        return [content['Key'] for content in response['Contents'] if content['Key'].endswith('.parquet')]
    else:
        return []

# Function to read a single Parquet file from S3
def read_parquet_from_s3(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    data = BytesIO(obj['Body'].read())
    return pd.read_parquet(data)

# Function to read and combine multiple Parquet files into a single DataFrame
def read_multiple_parquet_files(bucket_name, parquet_files):
    df_list = []
    for file_key in parquet_files:
        df = read_parquet_from_s3(bucket_name, file_key)
        df_list.append(df)
    # Combine all DataFrames into one
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df

# Streamlit app
def main():
    st.title("Matched 5x5 data")

    # Input the S3 bucket and prefix for the Parquet files
    bucket_name = "leadfuze-5x5-resolved"
    prefix = ""

    try:
        # List all Parquet files in the given prefix
        parquet_files = list_parquet_files(bucket_name, prefix)
        if parquet_files:
            st.write(f"Found {len(parquet_files)} records.")
            # Combine and display the content of all Parquet files
            combined_df = read_multiple_parquet_files(bucket_name, parquet_files)
            st.write(combined_df)
        else:
            st.warning("No Parquet files found in the specified path.")
    except Exception as e:
        st.error(f"Error reading Parquet files: {e}")

if __name__ == "__main__":
    main()
