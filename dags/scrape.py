from basketball_reference_scraper.teams import get_roster, get_team_stats, get_opp_stats, get_roster_stats, get_team_misc
import pandas as pd
import numpy as np
# import pyspark.pandas as ps
# from pyspark.sql import SparkSession
import json
from io import StringIO # python3; python2: BytesIO 
import boto3
# import s3fs

team_list =[{"team":"Atlanta Hawks","abbreviation":"ATL"},{"team":"Boston Celtics","abbreviation":"BOS"},{"team":"Brooklyn Nets","abbreviation":"BKN"},{"team":"Charlotte Hornets","abbreviation":"CHA"},{"team":"Chicago Bulls","abbreviation":"CHI"},{"team":"Cleveland Cavaliers","abbreviation":"CLE"},{"team":"Dallas Mavericks","abbreviation":"DAL"},{"team":"Denver Nuggets","abbreviation":"DEN"},{"team":"Detroit Pistons","abbreviation":"DET"},{"team":"Golden State Warriors","abbreviation":"GSW"},{"team":"Houston Rockets","abbreviation":"HOU"},{"team":"Indiana Pacers","abbreviation":"IND"},{"team":"LA Clippers","abbreviation":"LAC"},{"team":"Los Angeles Lakers","abbreviation":"LAL"},{"team":"Memphis Grizzlies","abbreviation":"MEM"},{"team":"Miami Heat","abbreviation":"MIA"},{"team":"Milwaukee Bucks","abbreviation":"MIL"},{"team":"Minnesota Timberwolves","abbreviation":"MIN"},{"team":"New Orleans Pelicans","abbreviation":"NOP"},{"team":"New York Knicks","abbreviation":"NYK"},{"team":"Oklahoma City Thunder","abbreviation":"OKC"},{"team":"Orlando Magic","abbreviation":"ORL"},{"team":"Philadelphia 76ers","abbreviation":"PHI"},{"team":"Phoenix Suns","abbreviation":"PHX"},{"team":"Portland Trail Blazers","abbreviation":"POR"},{"team":"Sacramento Kings","abbreviation":"SAC"},{"team":"San Antonio Spurs","abbreviation":"SAS"},{"team":"Toronto Raptors","abbreviation":"TOR"},{"team":"Utah Jazz","abbreviation":"UTA"},{"team":"Washington Wizards","abbreviation":"WAS"}]


# print(get_roster('NYK', 2025))
df = pd.DataFrame(team_list)
# new_df = df.rename(columns={"team": "NAME", "abbreviation": "ABV"})
# print(df)
player_list = pd.DataFrame()
# print(player_list)
for team in team_list:
 # print(team)
 raw_player = get_roster(team["abbreviation"],2025)
 player_df = pd.DataFrame(raw_player)
 player_df["TEAM"] = team["abbreviation"]
 # player_list._append(player_df)

 merged_df = pd.concat([player_list, player_df])
 player_list = merged_df
 # print(merged_df)
 



 blah = 0
 # player_list.to_csv('s3://baisleylake/NBA/players.csv', index=False)

s3_client = boto3.client('s3')
player_list.to_csv('players_.csv',index=False)
response = s3_client.upload_file('players_.csv', 'arn:aws:s3:us-east-1:129579558702:accesspoint/access', 'players.csv')
# player_list.to_csv('s3://baisleylake/NBA/players.csv', index=False)
