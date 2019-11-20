import concurrent.futures
import requests
import threading
import time
import configparser
import os
import logging
import logging.config
import sys
import yaml
import json
import sqlite3

thread_local = threading.local()

def setup_logging(default_path='loggingconfig.yaml', default_level=logging.INFO, env_key='LOG_CFG'):
    """Setting up the logging config"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
            except Exception as e:
                print('Error in Logging Configuration. Using default configs', e)
                logging.basicConfig(level=default_level, stream=sys.stdout)
    else:
        logging.basicConfig(level=default_level, stream=sys.stdout)
        print('Failed to load configuration file. Using default configs')

def create_db(db_file):
    """Create database for the collection of metadata from the JobMonitor API"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        logger.info("Connected to the database")
        cursor = conn.cursor()
        createTable = "CREATE TABLE "
    except Error as e:
        logger.error("Can't find the database")
    finally:
        if conn:
            conn.close()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def request_get(url):
    session = get_session()
    result = session.get(url).json()

def read_file():
    config = configparser.ConfigParser()
    dir = os.path.dirname(os.path.abspath(__file__))
    config.read(dir + os.sep + 'config.ini')
    site = config['default']['url']
    with open(config['default']['file'], 'r') as file:
        for f in file:
            final_url = f"{site}/{f}"
            request_get_all(final_url)
    return site

def request_get_all(site)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(request_get,site)

def main():
    """
    Main function of the program
    :return:
    """
    start_time = time.time()
    duration = time.time() - start_time
    logger.info(f"Collected {len(read_file())} in {duration} seconds")

if __name__ == "__main__":
    config = configparser.ConfigParser()
    dir = os.path.dirname(os.path.abspath(__file__))
    config.read(dir + os.sep + 'config.ini')
    setup_logging()  # Setting up the logging config
    logger = logging.getLogger(__name__)
    logger.info("Creating new log file")
    logger.info("Logging setup completed")
    create_db(config['default']['database'])
    main()

