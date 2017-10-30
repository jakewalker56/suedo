#https://www.draftkings.com/playbook/news/draftkings-lineup-upload-tool?sf53557139=1
#https://github.com/jaebradley/draftkings_client
#data.nba.com

import requests
import json
import csv
import pandas
import datetime
import time
from dateutil import tz

#list of available APIs from http://data.nba.net/10s/prod/v1/today.json:
base_url = "data.nba.net/10s"
debug = False
available_apis = {
	"listEndpoints": "/prod/v1/today.json",
	#only returns last 5 games from season...
	"playerGameLog": "/prod/v1/2017/players/{{personId}}_gamelog.json",
	"teamLeaders": "/prod/v1/2017/teams/{{teamUrlCode}}/leaders.json",
	"leagueRosterCoaches": "/prod/v1/2017/coaches.json",
	"leadTracker": "/prod/v1/{{gameDate}}/{{gameId}}_lead_tracker_{{periodNum}}.json",
	"scoreboard": "/prod/v2/{{gameDate}}/scoreboard.json",
	"recapArticle": "/prod/v1/{{gameDate}}/{{gameId}}_recap_article.json",
	"teamsConfig": "/prod/2017/teams_config.json",
	"playoffsBracket": "/prod/v1/2017/playoffsBracket.json",
	"currentScoreboard": "/prod/v1/20171022/scoreboard.json",
	#number of games to be played on each calendar day
	"calendar": "/prod/v1/calendar.json",
	"leagueSchedule": "/prod/v1/2017/schedule.json",
	"leagueUngroupedStandings": "/prod/v1/current/standings_all.json",
	"leagueLastFiveGameTeamStats": "/prod/v1/2017/team_stats_last_five_games.json",
	"leagueTeamStatsLeaders": "/prod/v1/2017/team_stats_rankings.json",
	"teamScheduleYear": "/prod/v1/{{seasonScheduleYear}}/teams/{{teamUrlCode}}/schedule.json",
	"gameBookPdf": "/prod/v1/{{gameDate}}/{{gameId}}_Book.pdf",
	"leagueMiniStandings": "/prod/v1/current/standings_all_no_sort_keys.json",
	"previewArticle": "/prod/v1/{{gameDate}}/{{gameId}}_preview_article.json",
	"teamSchedule": "/prod/v1/2017/teams/{{teamUrlCode}}/schedule.json",
	"teamLeaders2": "/prod/v1/2017/teams/{{teamId}}/leaders.json",
	"miniBoxscore": "/prod/v1/{{gameDate}}/{{gameId}}_mini_boxscore.json",
	"todayScoreboard": "/prod/v1/20171022/scoreboard.json",
	"anchorDate": "20171022",
	"leagueRosterPlayers": "/prod/v1/2017/players.json",
	"teamScheduleYear2": "/prod/v1/{{seasonScheduleYear}}/teams/{{teamId}}/schedule.json",
	"teamsConfigYear": "/prod/{{seasonScheduleYear}}/teams_config.json",
	"playerProfile": "/prod/v1/2017/players/{{personId}}_profile.json",
	"playerUberStats": "/prod/v1/2017/players/{{personId}}_uber_stats.json",
	"pbp": "/prod/v1/{{gameDate}}/{{gameId}}_pbp_{{periodNum}}.json",
	"leagueDivStandings": "/prod/v1/current/standings_division.json",
	"teams": "/prod/v1/2017/teams.json",
	"teamRoster": "/prod/v1/2017/teams/{{teamUrlCode}}/roster.json",
	"allstarRoster": "/prod/v1/allstar/2017/AS_roster.json",
	#http://data.nba.net/10s/prod/v1/20161001/11600001_boxscore.json
	"boxscore": "/prod/v1/{{gameDate}}/{{gameId}}_boxscore.json",
	"currentDate": "20171022",
	"playoffSeriesLeaders": "/prod/v1/2017/playoffs_{{seriesId}}_leaders.json",
	"leagueConfStandings": "/prod/v1/current/standings_conference.json"
}
def execute(query, params={}, retry=0):
    error = None
    try:
    	resp = requests.post("http://" + base_url + "/" + query, data=json.dumps(params),
                    headers={
                       'Content-Type':'application/json',
                       })
    except Exception as e:
    	#I think this is just my internet being wonky
    	print "Connection error({0}): {1}".format(e.errno, e.strerror)
    	error = "ConnectionError"
    if error == "ConnectionError" or resp.status_code != 200:
    	#internal server error; sleep 5 seconds then try again
    	print("failed query: http://" + base_url + "/" + query)
    	if not error:
    		print("code: " + str(resp.status_code))
    	time.sleep(5)   	
    	if retry < 4:
    		print("retrying...")
    		resp = execute(query, params, retry + 1)
    	else:
    		raise ValueError("failed retry too many times")
    return resp
    
def run_query(query, params={}):
    global debug
    resp = execute(query, params)
    if debug is True:
    	print(query)
    	print(resp)
    	print(resp.json())
    if resp.status_code != 200:
        # This means something went wrong.
        raise ValueError('POST ' + str(query) + ' - ' + str(params) + '/ {} / {}'.format(resp.status_code, resp.__dict__['_content']))
    return resp.json()

def replace_arg(query, argname, argvalue):
    return query.replace("{{" + str(argname) + "}}", str(argvalue))

def replace_year(query, new_year):
    year = str(new_year)
    return query.replace("/2017/", "/" + year +"/")

def parse_est_date_from_utc_time(string):
	#2015-10-03T02:30:00.000Z
	from_zone = tz.gettz('UTC')
	to_zone = tz.gettz('America/New_York')
	dt = datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%fZ')
	dt = dt.replace(tzinfo=from_zone)
	dt = dt.astimezone(to_zone)
	return dt.strftime("%Y%m%d")
	

def download_players(year=2017):
    print("Downloading players for " + str(year) + "...")
    players = run_query(replace_year(available_apis["leagueRosterPlayers"], year))
    players = players[players.keys()[1]]["standard"]
    player_data = pandas.DataFrame(columns=['id','first_name', 'last_name', 'dob', 'height', 'weight', 'pos', 
    	'draft_position', 'draft_year', 'country'])
    for player in players:
    	if not player["draft"]["roundNum"] or not player["draft"]["pickNum"]:
    		pick_num = "UNDRAFTED"
    	else:
    		pick_num = int(player["draft"]["pickNum"])
        if not player["heightInches"] or not player["heightFeet"] or not player["weightPounds"]:
            continue
    	player_data.loc[len(player_data)] = [str(player["personId"]), player["firstName"], player["lastName"], player["dateOfBirthUTC"], 
    										int(player["heightInches"]) + 12 * int(player["heightFeet"]), int(player["weightPounds"]),
    										player["pos"], pick_num, player["draft"]["seasonYear"], player["country"]]
    player_data.to_csv(str(year) + '_players.csv', index=False)

def download_games(year=2017):
	global debug
	print("Downloading games for " + str(year) + "...")
	schedule = run_query(replace_year(available_apis["leagueSchedule"], year))
	if debug:
		print(schedule[schedule.keys()[1]]["standard"])
		
	game_data = pandas.DataFrame(columns=['id','home_team_id','away_team_id','home_team_score','away_team_score','date'])
	for game in schedule[schedule.keys()[1]]["standard"]:
		if game["seasonStageId"] == "1":
			#skip the preseason games, because honestly...
			continue
		if game["hTeam"]["teamId"] == "1610616833" or game["hTeam"]["teamId"] == "1610616834":
			#ignore all star games where "EST" or "WST" were the home team
			continue
		if "startDateEastern" in game.keys():
			date = game["startDateEastern"]
		else:
			#older years don't have eastern starting times in schema
			date = parse_est_date_from_utc_time(game["startTimeUTC"])
		#make sure IDs are cast as strings so you don't drop the leading 0's
		game_data.loc[len(game_data)] = [str(game["gameId"]), str(game["hTeam"]["teamId"]), str(game["vTeam"]["teamId"]), game["hTeam"]["score"], 
										game["vTeam"]["score"], date]
	game_data.to_csv(str(year) + '_games.csv', index=False)

def download_teams(year = 2017):
	print("Downloading teams for " + str(year) + "...")
	teams = run_query(replace_year(available_apis["teamsConfig"], year))
	teams_data = pandas.DataFrame(columns=['id','name'])
	for team in teams["teams"]["config"]:
		if "ttsName" not in team.keys():
			print("ignoring:")
			print team
			#not an NBA team
			continue
		teams_data.loc[len(teams_data)] = [str(team["teamId"]), str(team["ttsName"])]
	teams_data.to_csv(str(year) + "_teams.csv", index=False)

def download_player_games(year=2017):
	games = get_games(year)
	print("Downloading player-games for " + str(year) + "...")
	player_games = pandas.DataFrame(columns=['player_id','game_id','team_id','fga', 'fgm', '3pa', '3pm', 'fta', 'ftm',
		'points','off_rebounds', 'def_rebounds', 'assists', 'turnovers', 'blocks', 'steals', 'fouls'])
	
	for index, game in games.iterrows():
		print('fetching game ' + str(game["id"]))
		boxscore = run_query(replace_arg(replace_arg(available_apis["boxscore"], "gameDate", game["date"]), "gameId", game["id"]))
		 # {u'pos': u'F', u'teamId': u'1610612744', u'offReb': u'1', u'pFouls': u'1', u'ftp': u'100', 
		 # u'min': u'18:34', u'personId': u'201142', u'totReb': u'4', u'fta': u'4', u'dnp': u'', u'ftm': u'4', 
		 # u'blocks': u'1', u'fgp': u'22.2', u'fgm': u'2', u'assists': u'3', u'fga': u'9', 
		 # u'steals': u'0', u'turnovers': u'3', u'plusMinus': u'0', u'tpa': u'4', u'tpm': u'1', 
		 # u'points': u'9', u'isOnCourt': False, u'tpp': u'25.0', u'defReb': u'3'}
		for player_row in boxscore["stats"]["activePlayers"]:
			player_games.loc[len(player_games)] = [str(player_row["personId"]), str(game["id"]), str(player_row["teamId"]),
			player_row["fga"], player_row["fgm"], player_row["tpm"], player_row["tpa"], player_row["ftm"], player_row["fta"],
			player_row["points"], player_row["offReb"], player_row["defReb"], player_row["assists"], player_row["turnovers"],
			player_row["blocks"], player_row["steals"], player_row["pFouls"]]
		#let's try to play nice with the API
		time.sleep(0.2)
	player_games.to_csv(str(year) + "_player_games.csv", index=False)

def get_games(year):
	try:
		games = pandas.read_csv(str(year) + "_games.csv", index_col = None, 
			converters={'id': lambda x: str(x), 'home_team_id': lambda x: str(x), 'away_team_id': lambda x: str(x)})
	except:
		download_games(year)
		games = pandas.read_csv(str(year) + "_games.csv", index_col = None, 
			converters={'id': lambda x: str(x), 'home_team_id': lambda x: str(x), 'away_team_id': lambda x: str(x)})
	return games

def get_players(year):
	try:
		players = pandas.read_csv(str(year) + "_players.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	except:
		download_players(year)
		players = pandas.read_csv(str(year) + "_players.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	return players

def get_teams(year):
	try:
		teams = pandas.read_csv(str(year) + "_teams.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	except:
		download_teams(year)
		teams = pandas.read_csv(str(year) + "_teams.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	return teams

def get_player_games(year):
	try:
		player_games = pandas.read_csv(str(year) + "_player_games.csv", index_col = None, 
			converters={'id': lambda x: str(x)})
	except:
		download_player_games(year)
		player_games = pandas.read_csv(str(year) + "_player_games.csv", index_col = None, 
			converters={'game_id': lambda x: str(x), 'player_id': lambda x: str(x), 'team_id': lambda x: str(x)})
	return player_games



def download_data(year=2017):
	download_games(year)
	download_players(year)
	download_teams(year)
	download_player_games(year)

def aggregate_data(year=2017):
	#TODO: merge everything into a single dataset we can build a model on (e.g. add aggregation columns like
	#last 5 game performance, home vs. away, player and opponent team ratings for offense, defense, rebounding, 
	#strength of schedule, etc.)
	#Make sure you only use data available at the time the game was played
	print("aggregated!")


download_players(2016)
aggregate_data(2016)

#TODO: can we get the betting odds / game line for these games retroactively?  
#There may be arbitrage between daily drafts and betting books
#Should we predict overtime games, since that leads to better numbers?  Or DOES it?
#TODO: download CARMELO player rankings from 538?



