

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session
# from sqlalchemy.orm import DeclarativeBase
# from sqlalchemy.orm import Mapped
# from sqlalchemy.orm import mapped_column
import pandas as pd
import numpy as np
# import pyspark.pandas as ps
# from pyspark.sql import SparkSession
from airflow.models.connection import Connection
import logging
import boto3
from botocore.exceptions import ClientError
# from basketball_reference_scraper.teams import get_roster #, get_team_stats, get_opp_stats, get_roster_stats, get_team_misc

# import pyspark.pandas as ps
# from pyspark.sql import SparkSession
import json
from io import StringIO # python3; python2: BytesIO 
import boto3
# import s3fs

aws_psql = Connection(
     conn_id="aws_psql",
     conn_type="mysql",
    #  description="connection description",
     host="35.153.83.72",
     login="admin",
     password="password",
    #  extra={"this_param": "some val", "that_param": "other val*"},
 )
@dag(
    start_date=datetime(2024, 11, 7),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Chaunce", "retries": 3},
    tags=["example"],
)
def upload_file():

    # Upload the file
    s3_client = boto3.client('s3')

    s3_client.upload_file('/home/cha/Desktop/Work/projects/airflow/players.csv', 'arn:aws:s3:us-east-1:129579558702:accesspoint/access', 'players.csv')

    return print('done')


upload_file()
