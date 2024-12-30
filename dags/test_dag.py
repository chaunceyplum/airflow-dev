

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
import os
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
def workflow():
    # Define tasks
    @task(
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Dataset("aep_profile")]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_aep_profile(**context) -> list[dict]:
        # try:
        
        print("execution code goes here")

        team_list =[{"team":"Atlanta Hawks","abbreviation":"ATL"},{"team":"Boston Celtics","abbreviation":"BOS"},{"team":"Brooklyn Nets","abbreviation":"BKN"},{"team":"Charlotte Hornets","abbreviation":"CHA"},{"team":"Chicago Bulls","abbreviation":"CHI"},{"team":"Cleveland Cavaliers","abbreviation":"CLE"},{"team":"Dallas Mavericks","abbreviation":"DAL"},{"team":"Denver Nuggets","abbreviation":"DEN"},{"team":"Detroit Pistons","abbreviation":"DET"},{"team":"Golden State Warriors","abbreviation":"GSW"},{"team":"Houston Rockets","abbreviation":"HOU"},{"team":"Indiana Pacers","abbreviation":"IND"},{"team":"LA Clippers","abbreviation":"LAC"},{"team":"Los Angeles Lakers","abbreviation":"LAL"},{"team":"Memphis Grizzlies","abbreviation":"MEM"},{"team":"Miami Heat","abbreviation":"MIA"},{"team":"Milwaukee Bucks","abbreviation":"MIL"},{"team":"Minnesota Timberwolves","abbreviation":"MIN"},{"team":"New Orleans Pelicans","abbreviation":"NOP"},{"team":"New York Knicks","abbreviation":"NYK"},{"team":"Oklahoma City Thunder","abbreviation":"OKC"},{"team":"Orlando Magic","abbreviation":"ORL"},{"team":"Philadelphia 76ers","abbreviation":"PHI"},{"team":"Phoenix Suns","abbreviation":"PHX"},{"team":"Portland Trail Blazers","abbreviation":"POR"},{"team":"Sacramento Kings","abbreviation":"SAC"},{"team":"San Antonio Spurs","abbreviation":"SAS"},{"team":"Toronto Raptors","abbreviation":"TOR"},{"team":"Utah Jazz","abbreviation":"UTA"},{"team":"Washington Wizards","abbreviation":"WAS"}]


        # print(get_roster('NYK', 2025))
        df = pd.DataFrame(team_list)
        # new_df = df.rename(columns={"team": "NAME", "abbreviation": "ABV"})
        # print(df)
        player_list = pd.DataFrame()
        # print(player_list)
        # for team in team_list:
        # # print(team)
        #     raw_player = get_roster(team["abbreviation"],2025)
        #     player_df = pd.DataFrame(raw_player)
        #     player_df["TEAM"] = team["abbreviation"]
        #     # player_list._append(player_df)

        #     merged_df = pd.concat([player_list, player_df])
        #     player_list = merged_df
        #     # print(merged_df)
        
        # player_list.to_csv('s3://baisleylake/NBA/players.csv', index=False)

        s3_client = boto3.client('s3')
        # player_list.to_csv('players_.csv',index=False)
        df.to_csv('teams_.csv',index=False)

        response = s3_client.upload_file('players_.csv', 'arn:aws:s3:us-east-1:129579558702:accesspoint/access', 'players.csv')
        # player_list.to_csv('s3://baisleylake/NBA/players.csv', index=False)

    
        # except:
        # print("API currently not available, using hardcoded data instead.")

        return player_list



# Instantiate the DAG
workflow()
