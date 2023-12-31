from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from get_data import OddsData, SportsNBA
from utils import init_logger
import pandas as pd
import ast

app = Flask(__name__)
api = Api(app)
odds = OddsData()
nba = SportsNBA()


class Sports(Resource):
    """
    wrapper class to pulling the sports data from the
    odds api
    """
    def get(self):
        all_sports = request.args.get('all')
        sports_json = odds.get_sports(all_sports=all_sports)
        return sports_json


class Scores(Resource):

    def get(self):
        sport = request.args.get('sport')
        days_from = request.args.get('daysFrom')
        scores_json = odds.get_scores(sport=sport,
                                      days_from=days_from)
        return scores_json


class Odds(Resource):

    def get(self):
        sport = request.args.get('sport')
        regions = request.args.get('regions')
        markets = request.args.get('markets')
        odds_format = request.args.get('odds_format')
        event_ids = request.args.get('event_ids')
        bookmakers = request.args.get('bookmakers')
        odds_json = odds.get_odds(sport=sport,
                                  regions=regions,
                                  markets=markets,
                                  odds_format=odds_format,
                                  event_ids=event_ids,
                                  bookmakers=bookmakers)
        return odds_json


class ReqsRemaining(Resource):

    def get(self):
        number_reqs = odds.get_remaining_req()
        return f"The number of remaining requests:  {number_reqs}"


class TeamsNba(Resource):

    def get(self):
        teams = nba.load_teams()
        return teams


class PlayersNba(Resource):

    def get(self):
        players = nba.load_players()
        return players


# odds api calls
api.add_resource(Sports, '/sports')  # link to get the sports
api.add_resource(Scores, '/scores')  # link to get the scores
api.add_resource(Odds, '/odds')  # link to get the odds
api.add_resource(ReqsRemaining, '/requests')  # link to get the odds

# NBA api calls
api.add_resource(TeamsNba, '/sports/nba/teams') # link to get nba teams
api.add_resource(PlayersNba, '/sports/nba/players') # link to get nba teams

if __name__ == '__main__':
    init_logger(name="app")
    app.run()