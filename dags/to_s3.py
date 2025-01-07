import logging
import boto3
from botocore.exceptions import ClientError
import os


def upload_file():

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file('/home/cha/Desktop/Work/projects/airflow/players.csv', 'arn:aws:s3:us-east-1:129579558702:accesspoint/access', 'airflow/players.csv')
    except ClientError as e:
        logging.error(e)
        return False
    return print('done')


upload_file()