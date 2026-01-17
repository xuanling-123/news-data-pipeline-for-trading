# Databricks notebook source
import requests
from datetime import datetime, timezone, timedelta
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import boto3

# COMMAND ----------

CATALOG = "coe_dev_dbricks_poc_usecases"
SCHEMA = "poc_databricks_explore"

# COMMAND ----------

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""

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

class NewsExtractor:
    """Extract news and save to Delta Lake"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.sources = {
            'newsapi': 'https://newsapi.org/v2/everything'
        }
    
    def extract_news(self, topics=['stocks', 'crypto', 'commodities']):
        """Extract news from APIs"""
        
        print("=" * 70)
        print("EXTRACTING NEWS DATA")
        print("=" * 70)
        
        articles = []

        for source, url in self.sources.items():
            for topic in topics:
                try:
                    response = requests.get(
                        url,
                        params={
                            'q': topic,
                            'language': 'en',
                            'sortBy': 'publishedAt',
                            'apiKey': self.api_key,
                            'pageSize': 100
                        },
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        for article in data.get('articles', []):
                            articles.append({
                                'source': source,
                                'topic': topic,
                                'title': article.get('title'),
                                'description': article.get('description'),
                                'content': article.get('content'),
                                'url': article.get('url'),
                                'author': article.get('author'),
                                'published_at': article.get('publishedAt'),
                                'extracted_at': datetime.now(timezone.utc).isoformat(),
                                'extraction_date': datetime.now(timezone.utc).strftime('%Y-%m-%d')
                            })
                        print(f"✅ Fetched {len(data.get('articles', []))} articles for {topic}")
                    else:
                        print(f"⚠️  Failed: {source}/{topic} - Status {response.status_code}")
                        
                except Exception as e:
                    print(f"❌ Error: {source}/{topic} - {str(e)}")
                    continue
        
        print(f"\n✅ Total extracted: {len(articles)} articles")
        return articles
    
    def save_to_bronze(self, articles):
        """Save raw data to Bronze layer (Delta Lake)"""
        
        if not articles:
            print("⚠️  No articles to save")
            return None
        
        # Convert to Spark DataFrame
        df = spark.createDataFrame(articles)
        
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", F.current_timestamp())
        df = df.withColumn("ingestion_date", F.current_date())
        
        # Write to Delta Lake (Bronze)
        df_bronze = (df.write
           .format("delta")
           .mode("append")
           .partitionBy("extraction_date", "topic")
           .option("mergeSchema", "true")
           .saveAsTable(f"{CATALOG}.{SCHEMA}.brz_news_data"))

        
        print(f"✅ Saved to Bronze: {f"{CATALOG}.{SCHEMA}.brz_news_data"}")
        
        return df_bronze

# COMMAND ----------


# Test extraction
extractor = NewsExtractor(NEWSAPI_KEY)
raw_articles = extractor.extract_news(topics=['bitcoin', 'gold', 'silver'])
bronze_df = extractor.save_to_bronze(raw_articles)

display(bronze_df)

# COMMAND ----------


