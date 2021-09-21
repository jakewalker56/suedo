import requests
import bs4
from bs4 import BeautifulSoup
import sys
import time

teams = ("ANA","HOU","OAK","TOR","ATL","MIL","STL","CHC","TBD","ARI","LAD","SFG","CLE","SEA","FLA","NYM","WSN","BAL","SDP","PHI","PIT","TEX","BOS","CIN","COL","KCR","DET","MIN","CHW","NYY","MIM","BEN","BOI")
years = range(1965, 2020)
draft_types = ("jansec", "janreg", "junsec") #, "junreg") 
sys.stdout.write("Draft,Year,Rnd,DT,OvPck,RdPck,Tm,Signed,Bonus,Name,Pos,WAR,G,AB,HR,BA,OPS,G,W,L,ERA,WHIP,SV,Type,Drafted Out of")
for team in teams:
    for year in years:
    	for draft_type in draft_types:
	        line = 0
	        querystring = "https://www.baseball-reference.com/draft/?team_ID=" + team + "&year_ID=" + str(year) + "&draft_type=" + draft_type + "&query_type=franch_year&from_type_jc=0&from_type_hs=0&from_type_4y=0&from_type_unk=0"
	        time.sleep(.1)
	        page = requests.get(querystring)
	        soup = BeautifulSoup(page.content,'html.parser')
	        for record in soup.find_all('tr'):
	            #print(record.contents)
	            for k in range(0, len(record.contents)):
	                if(type(record.contents[k]) == bs4.element.Tag):
	                    if("data-stat" in record.contents[k].attrs.keys()):
	                        if record.contents[k].attrs['data-stat'] == "year_ID":
	                            if line > 0:
	                                sys.stdout.write('\n') #signifies a new row in the table
	                                sys.stdout.write(draft_type + ",") #write the draft type since it doesn't show up as a field
	                            line += 1
	                        else:
	                            if line > 1:
	                                sys.stdout.write(',') #insert comma between values

	                        if line <= 1:
	                            continue
	                        #print(record.contents[k])
	                        #print(record.contents[k].attrs['data-stat'])
	                        tag=record.contents[k]
	                        if record.contents[k].attrs['data-stat'] in ("draft_round", "overall_pick", "pos", "came_from"):
	                            #sometimes can be blank
	                            if len(tag.contents) > 0:
	                                tag = tag.contents[0]
	                            
	                        if(len(tag.contents) > 0):
	                            if record.contents[k].attrs['data-stat'] == "player":
	                                if(type(tag.contents[0]) == bs4.element.NavigableString): #the player has an href link
	                                    val = str(tag.contents[0])[:-2]
	                                else:
	                                    val = str(tag.contents[0].contents[0])
	                            else:
	                                val = str(tag.contents[0])
	                            sys.stdout.write(val)