import pandas as pd
import os
import requests
import logging
import json
import sys
from pathlib import Path
from nba_api.stats.static import players
from nba_api.stats.static import teams
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.endpoints import teamyearbyyearstats

# constants for pulling data
API_KEY = os.environ["ODDS_API_KEY"]
HOST_URL = "https://api.the-odds-api.com"


class OddsData:
    """
    The Odds Data class is a wrapper class to pull data from the Odds api. Method calls
    are structured to preserve the number of calls by caching frequently used data sets.
    """

    def __init__(self, odds_format: str = 'american',
                 date_format: str = 'iso',
                 api_key: str = None):
        """
        Initializing the OddsData object for pulling data using the odds api
        documentation can be found here: https://the-odds-api.com/liveapi/guides/v4/#get-sports

        Parameters
        _____________________

            odds_format:
        Optional - Determines the format of odds in the response. Valid values are decimal and american.
        Defaults to decimal. When set to american, small discrepancies might exist for some bookmakers due to
        rounding errors.

            date_format:
        Optional - Determines the format of timestamps in the response. Valid values are unix and iso (ISO 8601).
        Defaults to iso.

            api_key: str
        The odds api key. If none is passed then default to what is persisted as an environment variable

        Returns
        ______________________

        None
        """
        self.odds_format = odds_format
        self.date_format = date_format
        if api_key is None:
            logging.info("Setting to the dev key")
            self._api_key = API_KEY
        else:
            logging.info(f"Setting to the prod key")
            self._api_key = api_key
        self._request_remaining = 0
        self._json_sports, self._df_sports = self._cache_sports()

    def get_data(self, end_point: str,
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
            self._request_remaining = int(api_response.headers['X-Requests-Remaining'])
            return None
        else:
            logging.info(f"Returning the {api_call_name} data")
            json_data = api_response.json()
            self._request_remaining = int(api_response.headers['X-Requests-Remaining'])
            return json_data


    def _cache_sports(self) -> any:
        """
        The get sports call to the odds api is the basis for all other calls
        to the API as the sports key is needed for odds and scores. This is used as a means to
        make one call only and cache to the results to help with limiting the number of calls to the
        api. The Free package allows only 500 calls a month

        Returns
        ______________
        pd.DataFrame, json
        """
        # get sports data
        logging.info("fetching sports data")
        parameters = {'api_key': API_KEY, 'all': 'true'}

        sports_json = self.get_data(end_point='/v4/sports/',
                                    params=parameters,
                                    api_call_name='sports')
        df_sports = pd.DataFrame(sports_json)
        # get the number of re
        return sports_json, df_sports

    def get_sports(self, all_sports: str = 'false') -> json:
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

            json return none if error code
        """
        # get sports data
        logging.info("Returning the sports data")
        if all_sports == 'true':
            return self._json_sports
        else:
            return self._df_sports[self._df_sports['active'] == True].to_json(orient='records')

    def get_scores(self, sport: str,
                   days_from: int=3) -> json:
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
                'api_key': self._api_key,
                'daysFrom': days_from,
                'dateFormat': self.date_format
            }

        scores_json = self.get_data(end_point=f'/v4/sports/{sport}/scores/',
                                    params=parameters,
                                    api_call_name='scores')
        logging.info(scores_json)

        return scores_json


    def get_odds(self,
                 sport: str,
                 regions: str = 'us',
                 markets: str = None,
                 event_ids: str = None,
                 bookmakers: str = None) -> json:
        """
        Function returns the odds for a selected sport. The parameters required
        to pull in data are sport and regions. Regions is defaulted to us. However, us, us2,
        uk, au, and eu are acceptable values for the parameter.

        Parameters
        ______________
            sport: str
        The sport key obtained from calling the /sports endpoint. upcoming is always valid,
        returning any live games as well as the next 8 upcoming games across all sports

            markets: str
        Optional - Determines which odds market is returned. Defaults to h2h (head to head / moneyline).
        Valid markets are h2h (moneyline), spreads (points handicaps), totals (over/under) and outrights (futures).
        Multiple markets can be specified if comma delimited. spreads and totals markets are mainly available for
        US sports and bookmakers at this time. Each specified market costs 1 against the usage quota, for each region.
        Returns a list of upcoming and live games with recent odds for a given sport, region and market

            oddsFormat: str
        Optional - Determines the format of odds in the response. Valid values are decimal and american.
        Defaults to decimal. When set to american, small discrepancies might exist for some bookmakers due to
        rounding errors.

            eventIds: str
        Optional - Comma-separated game ids. Filters the response to only return games for the specified game ids.

            bookmakers: str
        Optional - Comma-separated list of bookmakers to be returned. If both bookmakers and regions are both specified,
        bookmakers takes priority. Bookmakers can be from any region. Every group of 10 bookmakers is the equivalent of
        1 region. For example, specifying up to 10 bookmakers counts as 1 region. Specifying between 11 and 20 bookmakers
        counts as 2 regions.

        Returns:
        _____________
        json
        """
        # get sports data
        logging.info("fetching scores data")
        parameters = {
            'api_key': self._api_key,
            'regions': regions,
            'markets': markets,
            'dateFormat': self.date_format,
            'oddsFormat': self.odds_format,
            'eventIds': event_ids,
            'bookmakers': bookmakers
        }

        odds_json = self.get_data(end_point=f'/v4/sports/{sport}/odds/',
                                  params=parameters,
                                  api_call_name='odds')

        return odds_json

    def get_remaining_req(self):
        """ return the number of requests remaining """
        return self._request_remaining

    def get_odds_history(self,
                         sport: str,
                         odds_date: str,
                         regions: str = 'us',
                         markets: str = None
                         ) -> json:
        """
        Function pulls the odds data for a given sport on a given date and time.

        Parameters
        ______________
            sport: str
        The sport key obtained from calling the /sports endpoint. upcoming is always valid,
        returning any live games as well as the next 8 upcoming games across all sports

            odds_date: str
        The timestamp of the data snapshot to be returned, specified in ISO8601 format, for example
        2021-10-18T12:00:00Z The historical odds API will return the closest snapshot equal to or
        earlier than the provided date parameter.

            markets: str
        Optional - Determines which odds market is returned. Defaults to h2h (head to head / moneyline).
        Valid markets are h2h (moneyline), spreads (points handicaps), totals (over/under) and outrights (futures).
        Multiple markets can be specified if comma delimited. spreads and totals markets are mainly available for
        US sports and bookmakers at this time. Each specified market costs 1 against the usage quota, for each region.
        Returns a list of upcoming and live games with recent odds for a given sport, region and market

        Returns:
        _____________
        json
        """
        parameters = {'apiKey': self._api_key,
                      'regions': regions,
                      'markets': markets,
                      'date': odds_date}
        hist_odds = self.get_data(end_point=f'/v4/sports/{sport}/odds-history/',
                                  params=parameters,
                                  api_call_name='odds_history_nba')
        return hist_odds

class SportsNBA:
    """
    the Sports NBA class is a wrapper to the NBA API and returns stats using the api endpoints.
    Documentation to the nba_api can be found here: https://github.com/swar/nba_api/tree/master/docs/nba_api/stats/endpoints
    """

    def __init__(self):
        """
        initialize the sportsNBA obj for pulling data by pulling the static data needed for all other
        api calls
        """
        self._teams = teams.get_teams()
        self._players = players.get_players()

    def load_teams(self) -> json:
        """
        Return all the nba teams
        """
        return self._teams

    def load_players(self) -> json:
        """
        Return all the active and inactive nba players
        """
        return self._players
