import concurrent.futures
import requests
import threading
import configparser
import os
import logging
import logging.config
import sys
import yaml
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

def insert_to_db():
    logger.info("Start writing to SQLite.")
    conn = sqlite3.connect(config['default']['database'])
    cursor = conn.cursor()
    sql_create_table = ''' CREATE TABLE IF NOT EXISTS jobs (job_id text,id integer,app_name text,state text,date_created text); '''
    sql_insert = ''' INSERT INTO jobs(job_id,id,app_name,state,date_created) VALUES(:job_id,:id,:app_name,:state,:date_created); '''
    cursor.execute(sql_create_table)
    cursor.executemany(sql_insert, jobs)
    conn.commit()
    conn.close()
    logger.info("End writing to SQLite.")

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def get_job(job_id):
    session = get_session()
    logger.info(f"Getting job {job_id}.")
    with session.get(f"{config['default']['url']}{job_id}/") as response:
        global jobs
        jobs += response.json()

def request_all_jobs(job_ids, threads):
    logger.info("Start collecting jobs.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(threads)) as executor:
        executor.map(get_job, job_ids)
    logger.info("End collecting jobs.")

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


def main():
    """
    Main function of the program
    """
    with open(config['default']['file']) as file:
        logger.info("Reading text file.")
        job_ids = file.read().splitlines()
    request_all_jobs(job_ids, config['default']['threads'])
    insert_to_db()

if __name__ == "__main__":
    config = configparser.ConfigParser()
    dir = os.path.dirname(os.path.abspath(__file__))
    config.read(dir + os.sep + 'config.ini')
    setup_logging()  # Setting up the logging config
    logger = logging.getLogger(__name__)
    logger.info("Creating new log file")
    logger.info("Logging setup completed")
    jobs = []
    main()

