from airflow import DAG
from datetime import datetime, timedelta
import utils.constants
from airflow.decorators import dag, task

# define default args and constant yesterday
yesterday = datetime.now() - timedelta(days=1).replace(hour=0, minute=0, second=0, microsecond=0)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'catchup':False
}

# extract data from Reddit
@dag(default_args=default_args, schedule=None)
def extract_data():

    @task(task_id = 'extract_data')
    def extract_data(subreddit='dataengineering', time_filter='day', limit=100,file_name=f'reddit_{datetime.now().strftime('%Y%m%d')}'):
        pass


