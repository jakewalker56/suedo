import json
import csv
import numpy as np
import pandas as pd
import datetime
import time
from dateutil import tz
import warnings
import nba_utils
warnings.filterwarnings("ignore")


def aggregate_team_data_to_games(year=2016):
	#this method returns an array of game objects, where the game object contains all relevant information about both teams available before the game was played
	#time dependent facts about a team need to be attached to games, because they're actually facts about the team *when the game was played*
	#time independent facts about a team need to be attached to the team itself (not sure there are actually any cases of this?)
	teams = nba_utils.get_teams(year)
	games = nba_utils.get_games(year)
	#games = games[games["date"] < date]
	player_games = nba_utils.get_player_games(year)
	for game_index,game in games.iterrows():
		#id,home_team_id,away_team_id,home_team_score,away_team_score,date
		
		last_10_home_team_games = games[
				((games["home_team_id"] == game["home_team_id"]) |
				(games["away_team_id"] == game["home_team_id"])) &
				(games["date"] < game["date"])
			].sort_values(by=["date"]).head(10)
		if(len(last_10_home_team_games) < 1):
			print("no earlier home games found")
			games.at[game_index, "home_team_last_10_win_percentage"] = 0.5
			games.at[game_index, "home_team_last_10_average_points_scored"] = 100
			games.at[game_index, "home_team_last_10_average_points_allowed"] = 100
		else:
			home_team_last_10_won_games = \
				last_10_home_team_games[((last_10_home_team_games["home_team_id"] == game["home_team_id"]) & 
						(last_10_home_team_games["home_team_score"] > last_10_home_team_games["away_team_score"])) |
					((last_10_home_team_games["away_team_id"] == game["home_team_id"]) & 
						(last_10_home_team_games["home_team_score"] < last_10_home_team_games["away_team_score"]))]
			
			games.at[game_index, "home_team_last_10_win_percentage"] = \
				len(home_team_last_10_won_games) / \
				len(last_10_home_team_games)

			games.at[game_index, "home_team_last_10_average_points_scored"] = \
				(sum(last_10_home_team_games[last_10_home_team_games["home_team_id"] == game["home_team_id"]]["home_team_score"]) +
				sum(last_10_home_team_games[last_10_home_team_games["away_team_id"] == game["home_team_id"]]["away_team_score"])) / \
				len(last_10_home_team_games)
				
			games.at[game_index, "home_team_last_10_average_points_allowed"] = \
				(sum(last_10_home_team_games[last_10_home_team_games["home_team_id"] != game["home_team_id"]]["home_team_score"]) +
				sum(last_10_home_team_games[last_10_home_team_games["away_team_id"] != game["home_team_id"]]["away_team_score"])) / \
				len(last_10_home_team_games)

		last_10_away_team_games = games[
				((games["away_team_id"] == game["away_team_id"]) |
				(games["home_team_id"] == game["away_team_id"])) &
				(games["date"] < game["date"])
			].sort_values(by=["date"]).head(10)
		if(len(last_10_away_team_games) < 1):
			print("no earlier home games found")
			games.at[game_index, "away_team_last_10_win_percentage"] = 0.5
		else:
			away_team_last_10_won_games = \
				last_10_away_team_games[((last_10_away_team_games["away_team_id"] == game["away_team_id"]) & 
					(last_10_away_team_games["away_team_score"] > last_10_away_team_games["home_team_score"])) |
				((last_10_away_team_games["home_team_id"] == game["away_team_id"]) & 
					(last_10_away_team_games["away_team_score"] < last_10_away_team_games["home_team_score"]))]

			games.at[game_index, "away_team_last_10_win_percentage"] = \
				len(away_team_last_10_won_games) / \
				len(last_10_away_team_games)
			
			games.at[game_index, "away_team_last_10_average_points_scored"] = \
				(sum(last_10_away_team_games[last_10_away_team_games["home_team_id"] == game["away_team_id"]]["home_team_score"]) +
				sum(last_10_away_team_games[last_10_away_team_games["away_team_id"] == game["away_team_id"]]["away_team_score"])) / \
				len(last_10_away_team_games)
				
			games.at[game_index, "away_team_last_10_average_points_allowed"] = \
				(sum(last_10_away_team_games[last_10_away_team_games["home_team_id"] != game["away_team_id"]]["home_team_score"]) +
				sum(last_10_away_team_games[last_10_away_team_games["away_team_id"] != game["away_team_id"]]["away_team_score"])) / \
				len(last_10_away_team_games)

	
	#calculte last 5 game performance,  last game performance
	#self offense, defense, rebounding numbers
	#Aggregate player-level statistics into games (rebounds, %, free thorws, etc)
	#percentage of points scored on free throws
	#home vs. away performance
	#how many days of rest since last game
	#how many days of rest since last 2 games
	#how far they traveled since last game
	#whether it's been < 24 hours since their last game (include time of day, time zones)
	#merge betting odds from https://www.sportsbookreviewsonline.com/scoresoddsarchives/nba/nbaoddsarchives.htm

	#opponent offesne, defense, rebounding etc. when they play a given team vs their average
	#how much more or less a team scores than their opponents have given up in the last 10 games on average
	#how much more or less other teams score on an opponent than their last 10 average 
	#variance in scoring
	#incorporate a "# of players averaging > 15 points or > 6 assists not playing" metric?
	#incorporate a "tends to win close games" metric? (is clutch a thing?)
	#see if shift from open to close is useful - if we can predict it from other things we could arbitrage it? bet at open and counter bet at close...
	#if you can identify a shift toward or away from favorites, you could choose when to bet based on your model delta from favorites.  Also look to see if your model predicts direction of movement better than favorite/underdog
	
	#TODO: finish merging team level data
	#TODO: merge player_game data to get a single aggregated historical row per player
	#TODO: player data and include team data (self + oppoenent)
	#TODO: build a model using team and historical player data to predict player_game data
	
	return games

#TODO: can we get the betting odds / game line for these games retroactively?  
#There may be arbitrage between daily drafts and betting books
#Should we predict overtime games, since that leads to better numbers?  Or DOES it?
#TODO: download CARMELO player rankings from 538?
#time off between games
#referee prefences across players, techincal fouls, etc
#how do lines move over time?  when are they least efficient?
#player age?
#Altitude
#opposing players at the same position?


#get_data(2016)
#get_data(2017)

games_data = aggregate_team_data_to_games(2016)
game = games_data.iloc[500] #just pick a random game
print("home team: "+ str(nba_utils.get_team(game["home_team_id"])))
print("away team: "+ str(nba_utils.get_team(game["away_team_id"])))
print(game)
