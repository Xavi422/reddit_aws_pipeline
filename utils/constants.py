import configparser

parser = configparser.ConfigParser()

# Import constants from the config file
if parser.read('../config/config.conf'):
    
    # Reddit API credentials
    CLIENT_ID = parser.get('api_keys', 'reddit_client_id')
    CLIENT_SECRET = parser.get('api_keys', 'reddit_secret_key')

    #  AWS credentials and configurations
    AWS_ACCESS_KEY_ID = parser.get('aws','aws_access_key_id')
    AWS_SECRET_ACCESS_KEY = parser.get('aws','aws_secret_access_key')
    AWS_SESSION_TOKEN = parser.get('aws','aws_session_token')
    AWS_REGION = parser.get('aws','aws_region')
    AWS_S3_BUCKET = parser.get('aws','aws_bucket_name')

    # Postgres credentials and  configurations
    POSTGRES_HOST = parser.get('database','database_host')
    DATABASE_NAME = parser.get('database','database_name')
    DATABASE_PORT = parser.get('database','database_port')
    DATABASE_USERNAME = parser.get('database','database_username')
    DATABASE_PASSWORD = parser.get('database','database_password')

    # Other configurations
    INPUT_PATH = parser.get('file_paths','input_path')
    OUTPUT_PATH = parser.get('file_paths','output_path')

else:
    raise ValueError('Error: Config file not found. Config file should be located at ..config/config.conf')
