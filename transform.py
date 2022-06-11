import os
import boto3
import errno
import json
from datetime import datetime

buket='raw-data-bucket-bi'
s3=boto3.client('s3',aws_access_key_id='AKIAQPZIOPJXLYAQFNNS',aws_secret_access_key="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")
s3_client=boto3.client('s3',aws_access_key_id='AKIAQPZIOPJXLYAQFNNS',aws_secret_access_key="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")
objetosJson=['detalleEmpresas','gananciasAnuales','banlancesAnuales','historicos','cambioMoneda']

def assert_dir_exists(path):

    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def download_dir(client, bucket, path, target):

    # Handle missing / at end of prefix
    if not path.endswith('/'):
        path += '/'

    paginator = client.get_paginator('list_objects_v2')
    for result in paginator.paginate(Bucket=bucket, Prefix=path):
        # Download each file individually
        for key in result['Contents']:
            # Calculate relative path
            rel_path = key['Key'][len(path):]
            # Skip paths ending in /
            if not key['Key'].endswith('/'):
                local_file_path = os.path.join(target, rel_path)
                # Make sure directories exist
                local_file_dir = os.path.dirname(local_file_path)
                assert_dir_exists(local_file_dir)
                client.download_file(bucket, key['Key'], local_file_path)

                fecha = datetime.now().strftime("%Y%m%d%S%M%S")
                json_object = []
                for i in range(5):
                    s3_client.put_object(
                        Body=str(json.dumps(json_object)),
                        Bucket='raw-data-bucket-bi',
                        Key='api/alphavantage/' + objetosJson[i] + '/' + objetosJson[i] + '-' + fecha + '.json'
                    )

                print("Envio exitoso a S3")


download_dir(s3, buket, 'api', 'files')