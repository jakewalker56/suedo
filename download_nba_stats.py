#https://www.draftkings.com/playbook/news/draftkings-lineup-upload-tool?sf53557139=1
#https://github.com/jaebradley/draftkings_client
#data.nba.com

import requests
import json
import csv
import pandas

#list of available APIs from http://data.nba.net/10s/prod/v1/today.json:
base_url = "data.nba.net/10s"
all_players = None
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
	"boxscore": "/prod/v1/{{gameDate}}/{{gameId}}_boxscore.json",
	"currentDate": "20171022",
	"playoffSeriesLeaders": "/prod/v1/2017/playoffs_{{seriesId}}_leaders.json",
	"leagueConfStandings": "/prod/v1/current/standings_conference.json"
}

def run_query(query, params, debug=False):
    resp = requests.post("http://" + base_url + "/" + query, data=json.dumps(params),
                    headers={
                       'Content-Type':'application/json',
                       })
    if resp.status_code != 200:
        # This means something went wrong.
        raise ValueError('POST /' + query + ' - ' + params + '/ {} / {}'.format(resp.status_code, resp.__dict__['_content']))
    if debug is True:
    	print(query)
    return resp.json()

def replace_arg(query, argname, argvalue):
    return query.replace("{{" + argname + "}}", argvalue)

def replace_year(query, new_year):
    return query.replace("/2017/", "/" + new_year +"/")

def get_players():
    global all_players
    if all_players is None:
        players = run_query(available_apis["leagueRosterPlayers"], "")
        players = players[players.keys()[1]]["standard"]
        all_players = pandas.DataFrame.from_dict(players, orient='columns', dtype=None)
    return all_players

def get_player(first, last):
	players = get_players()
	return players.loc[(players['firstName'].str.lower() == first.lower()) & (players['lastName'].str.lower() == last.lower())]

def get_games(player_id, year=None, debug=False):
	if year is None:
		year = "2017"
	games = run_query(replace_year(replace_arg(available_apis["playerGameLog"], "personId", player_id), str(year)), "", debug)
	return games

#TODO: to find all the games for a player, probably need to go download ALL games and look at the box scores, 
#since player game log only reports last 5 games

player = get_player("Stephen", "Curry")
games = get_games(player["personId"].tolist()[0], 2015, True)
print (games)