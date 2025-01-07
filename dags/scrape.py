from basketball_reference_scraper.teams import get_roster, get_team_stats, get_opp_stats, get_roster_stats, get_team_misc
import pandas as pd
import numpy as np
# import pyspark.pandas as ps
# from pyspark.sql import SparkSession
import json
from io import StringIO # python3; python2: BytesIO 
import boto3
# import s3fs

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator

@dag(
    start_date=datetime(2024, 11, 7),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Chaunce", "retries": 3},
    tags=["example"],
)

def blah():
 team_list =[{"team":"Atlanta Hawks","abbreviation":"ATL"},{"team":"Boston Celtics","abbreviation":"BOS"},{"team":"Brooklyn Nets","abbreviation":"BKN"},{"team":"Charlotte Hornets","abbreviation":"CHA"},{"team":"Chicago Bulls","abbreviation":"CHI"},{"team":"Cleveland Cavaliers","abbreviation":"CLE"},{"team":"Dallas Mavericks","abbreviation":"DAL"},{"team":"Denver Nuggets","abbreviation":"DEN"},{"team":"Detroit Pistons","abbreviation":"DET"},{"team":"Golden State Warriors","abbreviation":"GSW"},{"team":"Houston Rockets","abbreviation":"HOU"},{"team":"Indiana Pacers","abbreviation":"IND"},{"team":"LA Clippers","abbreviation":"LAC"},{"team":"Los Angeles Lakers","abbreviation":"LAL"},{"team":"Memphis Grizzlies","abbreviation":"MEM"},{"team":"Miami Heat","abbreviation":"MIA"},{"team":"Milwaukee Bucks","abbreviation":"MIL"},{"team":"Minnesota Timberwolves","abbreviation":"MIN"},{"team":"New Orleans Pelicans","abbreviation":"NOP"},{"team":"New York Knicks","abbreviation":"NYK"},{"team":"Oklahoma City Thunder","abbreviation":"OKC"},{"team":"Orlando Magic","abbreviation":"ORL"},{"team":"Philadelphia 76ers","abbreviation":"PHI"},{"team":"Phoenix Suns","abbreviation":"PHX"},{"team":"Portland Trail Blazers","abbreviation":"POR"},{"team":"Sacramento Kings","abbreviation":"SAC"},{"team":"San Antonio Spurs","abbreviation":"SAS"},{"team":"Toronto Raptors","abbreviation":"TOR"},{"team":"Utah Jazz","abbreviation":"UTA"},{"team":"Washington Wizards","abbreviation":"WAS"}]

 df = pd.DataFrame(team_list)

 player_list = pd.DataFrame()
 for team in team_list:
  raw_player = get_roster(team["abbreviation"],2025)
  player_df = pd.DataFrame(raw_player)
  player_df["TEAM"] = team["abbreviation"]

  merged_df = pd.concat([player_list, player_df])
  player_list = merged_df

 s3_client = boto3.client('s3')
 player_list.to_csv('players_.csv',index=False)
 response = s3_client.upload_file('players_.csv', 'arn:aws:s3:us-east-1:129579558702:accesspoint/access', 's3://baisleylake/airflow/players.csv')
 # player_list.to_csv('s3://baisleylake/NBA/players.csv', index=False)
 return response