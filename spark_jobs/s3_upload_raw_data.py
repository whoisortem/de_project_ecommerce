import os
import boto3
from kaggle.api.kaggle_api_extended import KaggleApi
import shutil

def main():

    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_acess_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")
    raw_data_path = "olistbr/brazilian-ecommerce"

    api = KaggleApi()
    api.authenticate() 

    s3 = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_acess_key,
        aws_secret_access_key=s3_secret_key)

    raw_data_name = raw_data_path.split('/')[-1].replace('-','_')
    temp_directory = "/tmp/kaggle_ecommerce/"
    api.dataset_download_files(raw_data_path, path=temp_directory, unzip=True)

    for filename in os.listdir(temp_directory):
        if filename.endswith('.csv'):
            s3.upload_file(f"{temp_directory}/{filename}", "staging", f"{raw_data_name}/{filename}")
    shutil.rmtree(temp_directory, ignore_errors=True)

if __name__ == "__main__":
    main()