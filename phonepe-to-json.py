from pathlib import Path
import urllib.request
import json

def getPhonePeData(endpoint):
    response = None
    try:
        with urllib.request.urlopen(f'https://www.phonepe.com/pulse-static-api/v1/{endpoint}.json') as url:
            response = json.load(url)
    except Exception as err:
        print(f"{endpoint=}**{err=}")
    return response

country = getPhonePeData("map/transaction/hover/country/india/2022/3")
states = [item['name'].replace(" ", "-")
          for item in country['data']['hoverDataList']]
dirName = "data"
Path(dirName).mkdir(parents=True, exist_ok=True)
for year in range(2022, 2023): # Data available from 2018
    for quarter in range(1, 5):
        for state in states:
            for type1 in ['transaction', 'user']:
                for type2 in ['map', 'top', 'aggregated']:
                    hover = 'hover/' if type2 == 'map' else ''
                    endpoint = f'{type2}/{type1}/{hover}country/india/state/{state}/{year}/{quarter}'
                    resp = getPhonePeData(endpoint)
                    if resp is not None:
                        with open(f'{dirName}/{type1}-{year}-{quarter}-{state}-{type2}.json', 'w', encoding='utf-8') as f:
                            json.dump(resp, f, ensure_ascii=False, indent=4)
