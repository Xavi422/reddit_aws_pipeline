from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import sys
import os
import logging

OAUTH_URL = 'https://oauth.reddit.com/'
USER_AGENT = 'RedditDE/0.0.1 by u/Xavi422'
# set the parent folder of the dags folder to the first lookup location in the path which the interpreter searches for modules
# this folder would be the airflow folder in the docker container since you mounted the volume to the dags folder
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import CLIENT_ID, CLIENT_SECRET, REDDIT_USER_NAME, REDDIT_PASSWORD

# define default args and constant yesterday
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'catchup':False
}

# connect to Reddit and return Reddit instance
def get_access_token() -> str:
    try:
        # get access token
        auth = requests.auth.HTTPBasicAuth(CLIENT_ID,CLIENT_SECRET)
        # data for post request
        data = {'grant_type':'password', 'username':REDDIT_USER_NAME, 'password':REDDIT_PASSWORD}

        # headers for post request
        headers = {'User-Agent':USER_AGENT}

        # send post request
        res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
        
        if res.status_code != 200:
            raise Exception('Could not authenticate client. Check credentials')
        
        access_token = res.json().get('access_token')
        logging.info('Access token retrieved successfully')
        return access_token
        
    except Exception as e:
        raise

# extract posts from Reddit and store in dataframe
def extract_posts(access_token: str, subreddit: str, limit = None):
    # lists to store posts
    posts = []
    try:
        # headers for get request
        headers = {'User-Agent':USER_AGENT}
        headers['Authorization'] = f'bearer {access_token}'

        # parameters for get request
        after = None
        params = {'limit':limit,'after':after}

        while True:
            # send get request
            res = requests.get(f'{OAUTH_URL}r/{subreddit}/new', headers=headers, params=params)
            if res.status_code != 200:
                break
            
            # get data from response
            res_data = res.json()
            after = res_data['data']['after']

            for post in res_data['data']['children']:
                posts.append(post['data']['title'])
            
            if after is None:
                break
            
            params['after'] = after
        
        return posts
    except Exception as e:
        raise

# extract data from Reddit
@dag(default_args=default_args, schedule=None)
def extract_data_dag():

    @task(task_id = 'extract_data')
    def extract_data(subreddit='dataengineering', limit=100,file_name=f"reddit_{datetime.now().strftime('%Y%m%d')}"):
        posts = extract_posts(get_access_token(), subreddit, limit)
        print(posts[:5])

    extract_data()


# instantiate the DAG 
extract_data_dag()

