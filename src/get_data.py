import pandas as pd
import os
import requests
import logging
import json
import sys
from pathlib import Path

# constants for pulling data
API_KEY = os.environ["ODDS_API_KEY"]
ODDS_FORMAT = 'decimal' # decimal | american
DATE_FORMAT = 'iso' # iso | unix
HOST_URL = "https://api.the-odds-api.com"


def get_data(end_point: str,
             params: dict,
             api_call_name: str) -> json:
    """
    General function for grabbing data using the odds api call format
    Parameters:
    ________________

     end_point: str
    the endpoint of the rest api call to make i.e /v4/sports/ for sports data

     params:
    Parameters to feed the api call

     api_call_name:
    The name of the api call being made. This is purely for logging purposes
    :return:
    """
    # get data
    api_response = requests.get(
        f'{HOST_URL}{end_point}',
        params=params
    )

    if api_response.status_code != 200:
        logging.error(
            f'Failed to get data for {api_call_name}: status_code {api_response.status_code}, response body {api_response.text}')
        return None, None
    else:
        logging.info(f"Returning the {api_call_name} data")
        json_data = api_response.json()
        return json_data


def get_sports(all_sports: str = 'false') -> json:
    """
    function returns a list of in-season sport objects. The function output the sports in
    json, and pandas DataFrame

    Parameters:
    _________________

        all_sports: str
    if this parameter is set to true (all=true), a list of both in and out of season sports will be returned
    can be either 'true' or 'false'. default is set to 'false'


    Returns:
    _____________

        json, pd.DataFrame return none if error code
    """
    # get sports data
    logging.info("fetching sports data")
    parameters = {'api_key': API_KEY, 'all': all_sports}

    sports_json = get_data(end_point='/v4/sports/',
                           params=parameters,
                           api_call_name='sports')

    return sports_json


def get_scores(sport: str,
               days_from: int) -> json:
    """
    function returns a la list of upcoming, live and recently completed games for a given sport.
    Live and recently completed games contain scores.
    Games from up to 3 days ago can be returned using the daysFrom parameter.
    Live scores update approximately every 30 seconds.

    Parameters:
    _________________

        sport: str
    the sport key of the sport for scores
        days_from: int
    Value is between 1 and 3 days for the numebr of historical scores


    Returns:
    _____________

        json, pd.DataFrame return none if error code
    """
    # get sports data
    logging.info("fetching scores data")
    parameters ={
            'sport': sport,
            'api_key': API_KEY,
            'daysFrom': days_from,
            'dateFormat': DATE_FORMAT
        }

    scores_json = get_data(end_point=f'/v4/sports/',
                           params=parameters,
                           api_call_name='scores')

    return scores_json


def get_odds(sport: str,
             regions: str,
             markets: str,
             odds_format: str,
             event_ids: str,
             bookmakers: str) -> json:
    """
    Returns a list of upcoming and live games with recent odds for a given sport, region and market
    :return:
    """
    # get sports data
    logging.info("fetching scores data")
    parameters = {
        'sport': sport,
        'api_key': API_KEY,
        'regions': regions,
        'markets': markets,
        'dateFormat': DATE_FORMAT,
        'oddsFormat': odds_format,
        'eventIds': event_ids,
        'bookmakers': bookmakers
    }

    odds_json = get_data(end_point=f'/v4/sports/',
                         params=parameters,
                         api_call_name='odds')

    return odds_json