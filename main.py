import requests

baseUrl : str = "https://api.coincap.io/v2/"

def callAPI(api: str):
    response = requests.get(baseUrl + api)
    data = response.json()
    return data['data']

def initDatabase():
    from alchemy import initDatabase, seedDatabase
    initDatabase()
    seedDatabase()
    
def run_pipeline():
    from alchemy import fetch_change_percent, fetch_historic_prices, fetch_exchange_volume, fetch_markets_trades
    fetch_change_percent()
    fetch_historic_prices()
    fetch_exchange_volume()
    fetch_markets_trades()

if __name__ == "__main__":
    initDatabase()
    run_pipeline()