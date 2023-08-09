"""
The function is for pulling the raw historical odds data
for a given sport and persisting the raw data to a desired s3 bucket.
"""
import logging
import sys
import os
from datetime import datetime, date
from get_data import OddsData
from S3IO import S3IO
from utils import init_logger


SPORTS_KEY = 'basketball_nba'
PROFILE = os.environ["S3_PROFILE"]
BUCKET = os.environ["S3_ARB_BUCKET"]
API_KEY_PRD = os.environ["ODDS_API_KEY_PRD"]


def time_list(start_dt: date,
              end_dt: date) -> list:
    """
    Function creates a list of days in sequential order

    Parameters
    _______________
        start_dt: date
    The start date of the sequence

        end_dt: date
    The ending date of the sequence

    Return
    ______________
        list
    """
    return [
        date.fromordinal(i) for i in range(start_dt.toordinal(), end_dt.toordinal() + 1)
    ]


if __name__ == '__main__':
    init_logger(name='oddshistory1.log')
    # pass the to and from dates as inputs to the script
    from_dt = sys.argv[1]
    to_dt = sys.argv[2]

    from_dt = datetime.strptime(from_dt, '%Y-%m-%d').date()
    to_dt = datetime.strptime(to_dt, '%Y-%m-%d').date()

    # get the s3 access credentials
    s3_conn = S3IO(profile=PROFILE, bucket=BUCKET)

    # create the odds api object for pulling data
    odds = OddsData(api_key=API_KEY_PRD)

    # get the remaining requests left for the api call
    remaining_reqs = odds.get_remaining_req()
    number_errors = 0

    # get the number of days for each of the odds
    days_list = time_list(start_dt=from_dt, end_dt=to_dt)
    logging.info(f"Pulling {len(days_list)} number of days of data from {from_dt} to {to_dt}")

    # iterate over the days and parse the data
    for day in days_list:
        # check if the number of remaining reqs are enough
        if remaining_reqs <= 400:
            logging.error(f"Can not pull any more data, threshold for requests have been breached {remaining_reqs}")
            break

        if number_errors >= 5:
            logging.error(f"There were at least 5 errors in the process. Stoping for intervention")
            logging.info(f"Stopping the process. {day} is the date where process ended.")

        # get the historical data
        odds_date_format = str(day) + "T12:00:00Z"
        try:
            historical_data = odds.get_odds_history(sport=SPORTS_KEY, odds_date=odds_date_format)
            remaining_reqs = odds.get_remaining_req()
        except Exception as e:
            logging.error(f"There was an error loading in the data from the Odds Api for {odds_date_format}")
            number_errors += 1
            continue
        # load to the  s3 bucket if successful
        logging.info(f"Persisting data for {day}")
        nba_path = f"odds_data/raw_data/nba/nba_{day}.json"

        # load the data to s3 as a json file
        try:
            s3_conn.load_json(data=historical_data, file_path=nba_path)
            logging.info(f"Successfully loaded the data to s3 for {nba_path}")
        except Exception as e:
            logging.error(f"There was an issue loading the data to s3")
            number_errors += 1
    logging.info(f"Process Completed.")





