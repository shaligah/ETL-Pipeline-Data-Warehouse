import requests
import pandas as pd


def extract():
    limit = 30
    offset = 24892
    info =[]
    while True:
        url = f"https://api.jolpi.ca/ergast/f1/results.json?limit={limit}&offset={offset}"
        response = requests.get(url) #pull data from api
        if response.status_code!=200:
            raise Exception(f"failed at offset{offset}") #raise error code at page where error occured
        data= response.json()['MRData']['RaceTable']['Races']
        if not data: 
            break   #break if there is no more data to collect
            
        info.extend(data)
        offset+=limit
    return info
    
def transform_circuits(circuits):
    locations = []
    for place in circuits:
        locations.append({
            'CircuitId': place['Circuit']['circuitId'],
            'CircuitName': place['Circuit']["circuitName"],
            'Country': place['Circuit']['Location']['country'],
            'City': place['Circuit']['Location']["locality"],
            'Longitude': place['Circuit']['Location']['long'],
            'Latitude': place['Circuit']['Location']['lat'],
            'Season': place['season']
        })
    info =pd.DataFrame(locations)
    info = info.drop_duplicates()
    return info.reset_index(drop=True)

def transform_drivers(races):
    driver = []
    for race in races:
        for person in race['Results']:
            driver.append({
                'DriverId': person['Driver']['driverId'],
                'DriverName':f"{person['Driver']['givenName']} {person['Driver']['familyName']}",
                'DOB': person['Driver']['dateOfBirth'],
                'Nationality': person['Driver']['nationality'],
                'Number': person['Driver']['permanentNumber'],
                'Season' : race['season']
        })
    info = pd.DataFrame(driver)
    info = info.drop_duplicates()
    return info.reset_index(drop=True)

def transform_results(data):
    result = []
    for race in data:
        for end in race['Results']:
            result.append({
                'RaceName': race['raceName'],
                'Round': race['round'],
                'Season': race['season'],
                'CircuitName': race['Circuit']['circuitName'],
                'DriverName': f"{end['Driver']['givenName']} {end['Driver']['familyName']}",
                'grid': end['grid'],
                'laps':end['laps'],
                'status': end['status'],
                'position': end['position'],
                'Time': end.get('Time',{}).get('time'),
                'Constructor': end['Constructor']['name']
            })
    return pd.DataFrame(result)

def load_data(pairs,session):
    for data,table in pairs:
        for _, row in data.iterrows():
            doc = row.to_dict()
            session.merge(table(**doc))
    session.commit()
