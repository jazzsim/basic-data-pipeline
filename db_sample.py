import os
import psycopg2
from psycopg2 import pool
from psycopg2 import sql
from psycopg2 import OperationalError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# maintain a db connection
connection_pool = None

def initialize_pool():
    global connection_pool
    connection_pool = pool.SimpleConnectionPool(
        1,  # Minimum number of connections
        10, # Maximum number of connections
        dbname = "DE-Coincap",
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

def get_connection():
    global connection_pool
    if connection_pool is None:
        initialize_pool()
    return connection_pool.getconn()

def release_connection(conn):
    global connection_pool
    if connection_pool:
        connection_pool.putconn(conn)

def check_and_create_database() -> bool:
    """
    Check if the target database exists; if not, create it.
    :return: None
    """
    db_params = {
        'dbname': 'postgres',  # Connect to the default database
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
    }

    target_db = "DE-Coincap"

    # Connect to the default database
    conn = psycopg2.connect(**db_params)
    conn.autocommit = True  # Needed to create/drop databases

    # Check if the database exists
    try:
        with conn.cursor() as cursor:
            # Check if the database exists
            cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [target_db])
            result = cursor.fetchone()
            if result is not None:
                print(f"Database '{target_db}' already exists.")
            else:
                # Create the database using SQL query
                create_db_query = sql.SQL("CREATE DATABASE {dbname}").format(
                    dbname=sql.Identifier(target_db)
                )
                with conn.cursor() as cursor:
                    cursor.execute(create_db_query)
                    conn.commit()
                print(f"Database '{target_db}' created successfully.")
                return True
    except OperationalError as e:
        print(f"An error occurred: {e}")
    return False
      
def select_rows_query(query: str):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows : list = cursor.fetchall()
            return rows
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        release_connection(conn)
  
def create_table_query(query: str, index : str = None):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            
            if index:
                cursor.execute(index)
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        release_connection(conn)

def insert_rows_query(query: str, values: tuple, return_row: bool = False):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, values)
            
            # Commit the transaction
            conn.commit()
            if return_row:
                # Fetch the last inserted id
                return cursor.fetchone()
                
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        release_connection(conn)
        
def create_tables():
    # create Coins table
    create_table_query(
        """
        CREATE TABLE IF NOT EXISTS coins (
            id SERIAL PRIMARY KEY,
            coincap_id VARCHAR(255) NOT NULL UNIQUE,
            symbol VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            max_supply DECIMAL(30, 10)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS coins_coincap_id_idx ON coins (coincap_id);
        """
    )
    
    # create ChangePercent table
    create_table_query(
        """
        CREATE TABLE IF NOT EXISTS change_24h (
            id SERIAL PRIMARY KEY,
            coin_id INT NOT NULL,
            volumeUsd DECIMAL(30, 10) NOT NULL,
            percent DECIMAL(30, 10) NOT NULL,
            vwap DECIMAL(30, 10) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            FOREIGN KEY (coin_id) REFERENCES Coins(id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS change_24h_coin_id_idx ON change_24h (coin_id);
        """
    )
    
    # create HistoricPrices table
    create_table_query(
        """
        CREATE TABLE IF NOT EXISTS historic_prices (
            id SERIAL PRIMARY KEY,
            coin_id INT NOT NULL,
            price DECIMAL(30, 10) NOT NULL,
            unix_timestamp BIGINT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            FOREIGN KEY (coin_id) REFERENCES Coins(id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS historic_prices_coin_id_idx ON historic_prices (coin_id);
        """
    )
    
    # create Exchanges table
    create_table_query(
        """
        CREATE TABLE IF NOT EXISTS exchanges (
            id SERIAL PRIMARY KEY,
            coincap_id VARCHAR(255) NOT NULL UNIQUE,
            exchange_url VARCHAR(255) NOT NULL
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS exchanges_coincap_id_idx ON exchanges (coincap_id);
        """
    )
    
    # create ExchangeVolume table
    create_table_query(
        """
        CREATE TABLE IF NOT EXISTS exchange_volume (
            id SERIAL PRIMARY KEY,
            exchange_id INT NOT NULL,
            percent_total_volume DECIMAL(30, 10) NOT NULL,
            volume_usd DECIMAL(30, 10) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            FOREIGN KEY (exchange_id) REFERENCES Exchanges(id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS exchange_volume_exchange_id_idx ON exchange_volume (exchange_id);
        """
    )
    
    # create ExchangePairs table
    create_table_query(
        """
        CREATE TABLE IF NOT EXISTS exchange_pairs (
            id SERIAL PRIMARY KEY,
            exchange_id INT NOT NULL UNIQUE,
            base_symbol VARCHAR(255) NOT NULL,
            base_id VARCHAR(255) NOT NULL,
            quote_symbol VARCHAR(255) NOT NULL,
            quote_id VARCHAR(255) NOT NULL
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS exchange_pairs_exchange_id_idx ON exchange_pairs (id);
        """
    )
    
    # create MarketTrades
    create_table_query(
        """
        CREATE TABLE IF NOT EXISTS market_trades (
            id SERIAL PRIMARY KEY,
            exchange_id INT NOT NULL,
            price_quote DECIMAL(30, 10) NOT NULL,
            price_usd DECIMAL(30, 10) NOT NULL,
            trades_count_24h INT,
            percent_exchange_volume DECIMAL(30, 10),
            volume_24h DECIMAL(30, 10) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            FOREIGN KEY (exchange_id) REFERENCES Exchanges(id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS market_trades_exchange_id_idx ON market_trades (id);
        """
    )