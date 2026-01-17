# Databricks notebook source
# # Install required packages
# %pip install requests textblob python-dotenv

# # Download TextBlob corpora
# import nltk
# nltk.download('brown')
# nltk.download('punkt')

# # Restart Python kernel to use new packages
# dbutils.library.restartPython()

# COMMAND ----------

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""

# COMMAND ----------

import boto3
import json

# Initialize Secrets Manager client
session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
client = session.client(
    service_name='secretsmanager',
    region_name='us-east-1'
)
# Retrieve secret
secret_name = "news-data-ETL-pipeline-secrets-poonx"

try:
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(get_secret_value_response['SecretString'])
    
    NEWSAPI_KEY = secret['NEWSAPI_KEY']
    AWS_ACCESS_KEY_ID = secret['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = secret['AWS_SECRET_ACCESS_KEY']
    
    print("✅ Secrets loaded from AWS Secrets Manager")
    
except Exception as e:
    raise ValueError(f"Error retrieving secrets: {str(e)}")

# COMMAND ----------

