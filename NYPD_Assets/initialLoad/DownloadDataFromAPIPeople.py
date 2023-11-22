from mysecrets_config import api_key,api_secret
from calendar import Calendar, monthrange
import requests
from datetime import date
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone
import json
c = Calendar()
basic = HTTPBasicAuth(api_key,api_secret)
dict ={}
params = {}
response_list = []
for year in range(2016,2024):
    with open('./data/people_data/' +str(year) +'.json', 'a') as outfile:
        for mon in range(1,13):
            for dateT in list(c.itermonthdates(year, mon))[:monthrange(year, mon)[1]]:
                if dateT < date(2021, 12, 31) :  
                    print(dateT) 
                    params["crash_date"] = str("".join([str(dateT),"T00:00:00.000"]))
                    response = requests.get('https://data.cityofnewyork.us/resource/f55k-p6yu.json',params=params, auth=basic)
                    for item in response.json():
                        json.dump(item, outfile)
                        outfile.write('\n')
                    print(len(response.json()))
    outfile.close()
