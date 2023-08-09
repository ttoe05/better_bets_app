"""
The script is used to parse the json data and persist the data as a parquet file
in s3 with slight transformations to the data
"""

import logging
import os
import pandas as pd
from S3IO import S3IO
from utils import init_logger


PROFILE = os.environ["S3_PROFILE"]
BUCKET = os.environ["S3_ARB_BUCKET"]
s3_conn = S3IO(profile=PROFILE, bucket=BUCKET)
SPORT = "nba"
PERSIST_PATH = f"odds_data/transformed/{SPORT}/"
RAW_DATA_PATH = f"odds_data/raw_data/{SPORT}"


def parse_json(json_data: dict) -> pd.DataFrame:
    """
    The function parses the data gathered from the odds
    api

    Parameters
    ______________

        json_data: dict
    the json data for the odds on a sports team that will be parsed

    Return
    _____________
        pd.DataFrame
    """
    logging.info("parsing the historical odds json")
    # get the timestamp
    timestamp = json_data['timestamp']
    dfs = []
    # iterate over the list of data for the games on that day
    for game in json_data['data']:
        game_id = game['id']
        sport = game['sport_key']
        sport_title = game['sport_title']
        commencement = game['commence_time']
        home_team = game['home_team']
        away_team = game['away_team']
        bookmaker_key_list = []
        bookmaker_title_list = []
        market_key_list = []
        home_team_odds_list = []
        away_team_odds_list = []
        # iterate over the bookmakers
        for bookmaker in game['bookmakers']:
            bookmaker_key_list.append(bookmaker['key'])
            bookmaker_title_list.append(bookmaker['title'])
            for market in bookmaker['markets']:
                # skip the h2h_lay if it exists
                if market['key'] == 'h2h_lay':
                    continue
                market_key_list.append(market['key'])
                home_team_odds = market['outcomes'][0]['price'] if market['outcomes'][0]['name'] == home_team else \
                market['outcomes'][1]['price']
                away_team_odds = market['outcomes'][0]['price'] if market['outcomes'][0]['name'] == away_team else \
                market['outcomes'][1]['price']
                # append the odds to the list
                home_team_odds_list.append(home_team_odds)
                away_team_odds_list.append(away_team_odds)
        # create the dataframe and append it to the list of dfs
        data_values = {
            'bookmaker_key': bookmaker_key_list,
            'bookmaker_title': bookmaker_title_list,
            'market_key': market_key_list,
            'home_team_odds': home_team_odds_list,
            'away_team_odds': away_team_odds_list
        }
        for key in data_values.keys():
            logging.info(f"{key}:\t{len(data_values[key])}")
        try:
            daily_odds = pd.DataFrame(data_values)
            # add the constants
            daily_odds['game_id'] = game_id
            daily_odds['sport'] = sport
            daily_odds['sport_title'] = sport_title
            daily_odds['commencement'] = commencement
            daily_odds['home_team'] = home_team
            daily_odds['away_team'] = away_team
            dfs.append(daily_odds)
        except Exception as e:
            logging.error("Issue loading the game as a dataframe")
            logging.error(f"{e}")


    # append all the data
    df_data = pd.concat(dfs)

    # add the time constants
    df_data['timestamp'] = timestamp

    return df_data


if __name__ == '__main__':
    init_logger(name='data_transform')
    s3_conn = S3IO(profile=PROFILE, bucket=BUCKET)
    # get the list of json files that need to be parsed
    json_files = s3_conn.s3_list_obj(path="odds_data/raw_data/nba")
    logging.info(f"parsing {len(json_files)} files")
    for file in json_files:
        try:
            # load in the json data
            odds_json = s3_conn.read_json(file_path= file)
        except Exception as e:
            logging.error(f"Could not load the file {file}\n{e}")
            continue
        # perform the transformation and persist to s3
        logging.info(f"Parsing the data for file:\t{file}")
        try:
            df_odds = parse_json(json_data=odds_json)
        except Exception as e:
            logging.error(f"Issue with file {file}\n{e}")
            continue
        # persist the data to s3
        try:
            file_name = file.split("/")[-1].split(".")[0]
            file_path = PERSIST_PATH + file_name + ".parq"
            logging.info("Persisting file")
            s3_conn.s3_write_parquet(df=df_odds, file_path=file_path)
        except Exception as e:
            logging.error(f"There was an issue loading the file {file_path}\n{e}")

    logging.info("Transformation completed")
