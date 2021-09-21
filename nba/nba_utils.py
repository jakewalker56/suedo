import download
import json
import csv
import pandas
import datetime
import time
from dateutil import tz
import warnings

warnings.filterwarnings("ignore")

filepath = "../"

def parse_games(year):
	games = pandas.read_csv(filepath + "data/" + str(year) + "_games.csv", index_col = None, 
			converters={'id': lambda x: str(x), 'home_team_id': lambda x: str(x), 'away_team_id': lambda x: str(x)},
			parse_dates=["date"],
			date_parser = (lambda x: pandas.datetime.strptime(x, '%Y%m%d')))
	return(games)

def parse_players(year):
	players = pandas.read_csv(filepath + "data/" + str(year) + "_players.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	return(players)

def parse_teams(year):
	teams = pandas.read_csv(filepath + "data/" + str(year) + "_teams.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	return(teams)

def parse_player_games(year):
	player_games = pandas.read_csv(filepath + "data/" + str(year) + "_player_games.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	return(player_games)

def get_games(year):
	try:
		games = parse_games(year)
	except:
		download.download_games(year)
		games = parse_games(year)
	return games

def get_players(year):
	try:
		players = parse_players(year)
	except:
		download.download_players(year)
		players = parse_players(year)
	return players

def get_teams(year):
	try:
		teams = parse_teams(year)
	except:
		download.download_teams(year)
		teams = parse_teams(year)
	return teams

def get_player_games(year):
	try:
		player_games = parse_player_games(year)
	except:
		download.download_player_games(year)
		player_games = parse_player_games(year)
	return player_games

def get_team(id, year=2016):
	teams = get_teams(year)
	return teams[teams["id"] == id].head(1)