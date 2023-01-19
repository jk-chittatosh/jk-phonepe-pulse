import json
import urllib.request
from pathlib import Path
from pymongo import MongoClient

def getPhonePeData(endpoint):
    response = None
    try:
        with urllib.request.urlopen(f'https://www.phonepe.com/pulse-static-api/v1/{endpoint}.json') as url:
            response = json.load(url)
    except Exception as apiErr:
        print(f"{endpoint=}**{apiErr=}")
    return response

client = MongoClient("mongodb+srv://phonepeDataReadWrite:0AbjxBQrsF7n7HiD@cesspool-ft7rs.mongodb.net/phonepe-upi-usage?authSource=admin&replicaSet=cesspool-shard-0&w=majority&readPreference=secondary&retryWrites=true&ssl=true")
db = client['phonepe-upi-usage']
country = getPhonePeData("map/transaction/hover/country/india/2022/3")
states = [item['name'].replace(" ", "-")
          for item in country['data']['hoverDataList']]
dirName = "error-data"
Path(dirName).mkdir(parents=True, exist_ok=True)
for year in range(2018, 2023):
    for quarter in range(1, 5):
        for state in states:
            common = {'year': year, 'quarter': quarter, 'state': state}
            for type1 in ['transaction', 'user']:
                for type2 in ['map', 'top', 'aggregated']:
                    hover = 'hover/' if type2 == 'map' else ''
                    endpoint = f'{type2}/{type1}/{hover}country/india/state/{state}/{year}/{quarter}'
                    resp = getPhonePeData(endpoint)
                    if resp is not None:
                        try:
                            if (type1 == 'transaction' and type2 == 'aggregated'):
                                docs = [{**common, 'name': item['name'], **item['paymentInstruments'][0]} for item in resp['data']['transactionData']]
                            elif (type1 == 'transaction' and type2 == 'map'):
                                docs = [{**common, 'name': item['name'], **item['metric'][0]} for item in resp['data']['hoverDataList']]
                            elif (type1 == 'transaction' and type2 == 'top'):
                                districts = [{**common, 'locationType': 'district', 'entityName': item['entityName'], **item['metric']} for item in resp['data']['districts']]
                                pincodes = [{**common, 'locationType': 'pincode', 'entityName': item['entityName'], **item['metric']} for item in resp['data']['pincodes']]
                                docs = [*districts, *pincodes]
                            if (type1 == 'user' and type2 == 'aggregated'):
                                if resp['data']['usersByDevice'] is None:
                                    docs = [{**common, **resp['data']['aggregated']}]
                                else:
                                    docs = [{**common, **resp['data']['aggregated'], **item} for item in resp['data']['usersByDevice']]
                            elif (type1 == 'user' and type2 == 'map'):
                                docs = [{**common, 'name': key, **resp['data']['hoverData'][key]} for key in resp['data']['hoverData']]
                            elif (type1 == 'user' and type2 == 'top'):
                                districts = [{**common, **item} for item in resp['data']['districts']]
                                pincodes = [{**common, **item} for item in resp['data']['pincodes']]
                                docs = [*districts, *pincodes]
                            mongoResp = db[f'{type1}s{type2.title()}'].insert_many(docs)
                        except Exception as dataErr:
                            print(f"{endpoint=}**{dataErr=}")
                            with open(f'{dirName}/{type1}-{year}-{quarter}-{state}-{type2}.json', 'w', encoding='utf-8') as f:
                                json.dump(resp, f, ensure_ascii=False, indent=4)