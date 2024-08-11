from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from praw import Reddit
import sys
import os
import logging

# set the parent folder of the dags folder to the first lookup location in the path which the interpreter searches for modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import CLIENT_ID, CLIENT_SECRET

# define default args and constant yesterday
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'catchup':False
}

# connect to Reddit and return Reddit instance
def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = Reddit(client_id=client_id,
                        client_secret=client_secret,
                        user_agent=user_agent)
        logging.info('Connected to Reddit')
        return reddit
        
    except Exception as e:
        raise

# extract posts from Reddit
def extract_posts(reddit_instanct: Reddit, subreddit: str, time_filter: str, limit = None):
    subreddit = reddit_instanct.subreddit(subreddit)
    posts = subreddit.top(time_filter, limit=limit)

def reddit_pipeline(filename: str, subreddit: str, time_filter = 'day', limit = None):
    #create reddit instance
    instance = connect_reddit(CLIENT_ID, CLIENT_SECRET, 'Xavier Agent')

    # extraction of data
    posts = extract_posts(instance, subreddit, time_filter, limit)
    print(posts)

# extract data from Reddit
@dag(default_args=default_args, schedule=None)
def extract_data_dag():

    @task(task_id = 'extract_data')
    def extract_data(subreddit='dataengineering', time_filter='day', limit=100,file_name=f"reddit_{datetime.now().strftime('%Y%m%d')}"):
        reddit_pipeline(file_name, subreddit, time_filter, limit)

    extract_data()


# create the DAG   
dag = extract_data_dag()

