"""
Pulling nba historical data using the nba api
"""
import logging
import sys
import os
import pandas as pd
from datetime import datetime, date
from get_data import SportsNBA
from S3IO import S3IO
from utils import init_logger


PROFILE = os.environ["S3_PROFILE"]
BUCKET = os.environ["S3_ARB_BUCKET"]
BASE_PATH = "nba_data/raw_data/"

if __name__ == '__main__':
    # initialize the logger
    init_logger(name='nba_historical')
    nba = SportsNBA()

    # get the list of teams
    df_teams = pd.DataFrame(nba.load_teams())
    logging.info(df_teams.head())

    # initialize the list of dataframes for each season
    season_22 = []
    season_23 = []

    # iterate over the team ids to get the 2022-2023 season and the 2021-2022 seasons
    for team_id in df_teams['id']:
        logging.info(f"Getting the season data for team {team_id}")
        try:
            df_game_stats = nba.get_team_game_stats(team_id=team_id)
        except Exception as e:
            logging.error(f"Could not load team {team_id}\n{e}")
            continue
        # convert the date to datetime for filtering
        df_game_stats['GAME_DATE'] = pd.to_datetime(df_game_stats['GAME_DATE'])
        # filter the 2022 season and append it to the list
        df_23 = df_game_stats[(df_game_stats['GAME_DATE'] >= '2022-10-18') & (df_game_stats['GAME_DATE'] <= '2023-06-18')]
        df_22 = df_game_stats[(df_game_stats['GAME_DATE'] >= '2021-10-18') & (df_game_stats['GAME_DATE'] <= '2022-06-18')]

        logging.info(f"Shape of the team {team_id} DataFrame for the 2022 season {df_22.shape}")
        logging.info(f"Shape of the team {team_id} DataFrame for the 2023 season {df_23.shape}")

        # append the data
        season_22.append(df_22)
        season_23.append(df_23)

    # concat the datasets together
    df_season_22 = pd.concat(season_22)
    df_season_23 = pd.concat(season_23)

    logging.info(f"Shape of the 2022 season dataset:\t{df_season_22.shape}")
    logging.info(f"Shape of the 2023 season dataset:\t{df_season_23.shape}")

    # get the s3 access credentials
    s3_conn = S3IO(profile=PROFILE, bucket=BUCKET)

    # persist the data to s3
    logging.info("persisting the 2022 season stats")
    try:
        file_path = BASE_PATH + "2021_2022.parq"
        s3_conn.s3_write_parquet(df=df_season_22, file_path=file_path)
    except Exception as e:
        logging.info(f"Could not load the 2022 season to s3\n{e}")

    logging.info("persisting the 2023 season stats")
    try:
        file_path = BASE_PATH + "2022_2023.parq"
        s3_conn.s3_write_parquet(df=df_season_23, file_path=file_path)
    except Exception as e:
        logging.info(f"Could not load the 2023 season to s3\n{e}")

