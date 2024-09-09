import os
import pandas as pd

from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import DECIMAL, Column, ForeignKey, Integer, Numeric, String, Table, create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from main import callAPI

load_dotenv()
Base = declarative_base()
metadata_obj = MetaData()

# Define the PostgreSQL connection URI
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = 'DE-Coincap'

DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URI, echo=True)


def initDatabase():
    # coin
    Table(
        "coins",
        metadata_obj,
        Column("id", String(255), primary_key=True),
        Column("symbol", String(255)),
        Column("name", String(255)),
        Column("max_supply", DECIMAL(30, 10), nullable=True),
    )
    
    # change_24h
    Table(
        "change_24h",
        metadata_obj,
        Column("coin_id", String(255) , ForeignKey("coins.id")),
        Column("volumeUsd", DECIMAL(30, 10)),
        Column("percent", DECIMAL(30, 10)),
        Column("vwap", DECIMAL(30, 10)),
        Column("timestamp", String(255)),
    )
    
    # historic_prices
    Table(
        "historic_prices",
        metadata_obj,
        Column("coin_id", String(255) , ForeignKey("coins.id")),
        Column("price", DECIMAL(30, 10)),
        Column("unix_timestamp", Numeric(20, 0)),
        Column("timestamp", String(255)),
    )
    
    # Exchanges
    Table(
        "exchanges",
        metadata_obj,
        Column("id", String(255) , primary_key=True),
        Column("exchange_url", String(255)),
    )
    
    #Exchange Volume
    Table(
        "exchange_volume",
        metadata_obj,
        Column("exchange_id", String(255) , ForeignKey("exchanges.id")),
        Column("percent_total_volume", DECIMAL(30, 10)),
        Column("volume_usd", DECIMAL(30, 10)),
        Column("timestamp", String(255)),
    )
    
    # Exchange Pairs
    Table(
        "exchange_pairs",
        metadata_obj,
        Column("exchange_id", String(255) , ForeignKey("exchanges.id")),
        Column("base_symbol", String(255)),
        Column("base_id", String(255)),
        Column("quote_symbol", String(255)),
        Column("quote_id", String(255)),
    )
    
    # Market Trades
    Table(
        "market_trades",
        metadata_obj,
        Column("exchange_pair_id", String(255) , ForeignKey("exchanges.id")),
        Column("price_quote", DECIMAL(30, 10)),
        Column("price_usd", DECIMAL(30, 10)),
        Column("percent_exchange_volume", DECIMAL(30, 10), nullable=True),
        Column("volume_24h", DECIMAL(30, 10)),
        Column("trades_count_24h", Integer, nullable=True),
        Column("timestamp", String(255)),
    )
    
    metadata_obj.create_all(engine)

def seedDatabase():
    coins = get_table('coins')
    if len(coins) == 0:
        seed_coins()
        
    exchanges = get_table('exchanges')
    if len(exchanges) == 0:
        seed_exchanges()
    
def seed_coins():
    df = fetch_data("assets" + "?limit=50")
    df = df[['id', 'symbol', 'name', 'maxSupply']]
    df.rename(columns={'maxSupply': 'max_supply'}, inplace=True)
    df.to_sql('coins', engine, if_exists='append', index=False)

    # # seed historical as well
    [seed_historic(x) for x in df['id']]

def seed_historic(coin_id: str):
    df = fetch_data("assets" + "/" + coin_id + "/history?interval=d1")
    df = df[['priceUsd', 'time', 'date']]
    df['coin_id'] = coin_id
    df.rename(columns={'priceUsd': 'price'}, inplace=True)
    df.rename(columns={'time': 'unix_timestamp'}, inplace=True)
    df.rename(columns={'date': 'timestamp'}, inplace=True)
    df.to_sql('historic_prices', engine, if_exists='append', index=False)

def seed_exchanges():
    df = fetch_data("exchanges")
    df = df[:20]
    df = df[['exchangeId', 'exchangeUrl']]
    df.rename(columns={'exchangeId': 'id'}, inplace=True)
    df.rename(columns={'exchangeUrl': 'exchange_url'}, inplace=True)
    df.to_sql('exchanges', engine, if_exists='append', index=False)
    
    # seed exchange pairs as well
    seed_exchange_pairs(df)
    
def seed_exchange_pairs(exchange_df: pd.DataFrame):
    df = fetch_data("markets" + "?assetId=bitcoin" + "&quoteId=tether")
    df = df[['exchangeId', 'baseSymbol', 'baseId', 'quoteSymbol', 'quoteId']]
    
    # only add the ones that are in the exchange_df
    matched_df = df[df['exchangeId'].isin(exchange_df['id'])]

    matched_df.rename(columns={'exchangeId': 'exchange_id'}, inplace=True)
    matched_df.rename(columns={'baseSymbol': 'base_symbol'}, inplace=True)
    matched_df.rename(columns={'baseId': 'base_id'}, inplace=True)
    matched_df.rename(columns={'quoteSymbol': 'quote_symbol'}, inplace=True)
    matched_df.rename(columns={'quoteId': 'quote_id'}, inplace=True)
    matched_df.to_sql('exchange_pairs', engine, if_exists='append', index=False)

def fetch_change_percent():
    rows = get_table('coins')
    for row in rows:
        df = fetch_data("assets" + "/" + row.id)
        df = df[['volumeUsd24Hr','changePercent24Hr', 'vwap24Hr']]
        df['coin_id'] = row.id
        df['timestamp'] = datetime.now()
        df.rename(columns={'volumeUsd24Hr': 'volumeUsd'}, inplace=True)
        df.rename(columns={'changePercent24Hr': 'percent'}, inplace=True)
        df.rename(columns={'vwap24Hr': 'vwap'}, inplace=True)
        df.to_sql('change_24h', engine, if_exists='append', index=False)

def fetch_historic_prices():
    rows = get_table('coins')
    for row in rows:
        df = fetch_data("assets" + "/" + row.id + "/history?interval=d1")
        # get last row of dataframe as dataframe 
        df = df.tail(1)
        df['coin_id'] = row.id
        df.rename(columns={'priceUsd': 'price'}, inplace=True)
        df.rename(columns={'time': 'unix_timestamp'}, inplace=True)
        df.rename(columns={'date': 'timestamp'}, inplace=True)
        df.to_sql('historic_prices', engine, if_exists='append', index=False)

def fetch_exchange_volume():
    rows = get_table('exchanges')
    for row in rows:
        df = fetch_data("exchanges" + "/" + row.id)
        df = df[['exchangeId', 'percentTotalVolume', 'volumeUsd']]
        df['exchange_id'] = row.id
        df['timestamp'] = datetime.now()
        df.rename(columns={'percentTotalVolume': 'percent_total_volume'}, inplace=True)
        df.rename(columns={'volumeUsd': 'volume_usd'}, inplace=True)
        
        df.drop(columns=['exchangeId'], inplace=True)
        df.to_sql('exchange_volume', engine, if_exists='append', index=False)
        
def fetch_markets_trades():
    rows = get_table('exchange_pairs')
    exchange_pairs_df = pd.DataFrame(rows)
    df = fetch_data("markets" + "?assetId=bitcoin" + "&quoteId=tether")
    df.drop(columns=['exchangeId'], inplace=True)

    for row in rows:
        # only add the ones that are in the exchange_pairs_df
        matched_df = df[df['exchangeId'].isin(exchange_pairs_df['exchange_id'])]
        matched_df['exchange_pair_id'] = row.exchange_id
        matched_df.rename(columns={'priceQuote': 'price_quote'}, inplace=True)
        matched_df.rename(columns={'priceUsd': 'price_usd'}, inplace=True)
        matched_df.rename(columns={'percentExchangeVolume': 'percent_exchange_volume'}, inplace=True)
        matched_df.rename(columns={'volumeUsd24Hr': 'volume_24h'}, inplace=True)
        matched_df.rename(columns={'tradesCount24Hr': 'trades_count_24h'}, inplace=True)
        matched_df['timestamp'] = datetime.now()
        matched_df.to_sql('market_trades', engine, if_exists='append', index=False)
    
def get_table(table_name: str):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    table = Table(table_name, metadata_obj, autoload_with=engine)
    rows = session.query(table).all()
    
    session.close()
    return rows

def fetch_data(api : str) -> pd.DataFrame:
    data = callAPI(api)
    if type(data) is list:
        return pd.DataFrame(data)
    else:
        return pd.DataFrame([data])