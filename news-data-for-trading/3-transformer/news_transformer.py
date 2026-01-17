# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from textblob import TextBlob
import re

# COMMAND ----------

CATALOG = "coe_dev_dbricks_poc_usecases"
SCHEMA = "poc_databricks_explore"
BRONZE_PATH = f"{CATALOG}.{SCHEMA}.brz_news_data"

# COMMAND ----------

class NewsTransformer:
    """Transform news data using Spark"""
    
    def __init__(self):
        self.commodity_keywords = [
            'gold', 'silver', 'copper', 'oil', 'crude', 'natural gas',
            'wheat', 'corn', 'soybeans', 'coffee', 'sugar', 'cotton',
            'platinum', 'palladium', 'aluminum'
        ]
    
    def transform(self, bronze_path=BRONZE_PATH):
        """Transform Bronze to Silver layer"""
        
        print("=" * 70)
        print("TRANSFORMING NEWS DATA (SPARK)")
        print("=" * 70)
        
        # Read from Bronze
        df = spark.table(bronze_path)
        
        print(f"📥 Loaded {df.count()} articles from Bronze")
        
        # 1. Clean text (Spark UDF)
        @F.udf(StringType())
        def clean_text(text):
            if not text:
                return ""
            # Remove HTML
            text = re.sub('<.*?>', '', text)
            # Remove extra whitespace
            text = ' '.join(text.split())
            return text
        
        df = df.withColumn("clean_title", clean_text(F.col("title")))
        df = df.withColumn("clean_content", clean_text(F.col("content")))
        
        # 2. Sentiment analysis (Spark UDF)
        @F.udf(StructType([
            StructField("polarity", FloatType()),
            StructField("subjectivity", FloatType()),
            StructField("label", StringType())
        ]))
        def analyze_sentiment(title, description, content):
            text = f"{title or ''} {description or ''} {content or ''}"
            
            if not text.strip():
                return (0.0, 0.0, "neutral")
            
            blob = TextBlob(text)
            polarity = float(blob.sentiment.polarity)
            subjectivity = float(blob.sentiment.subjectivity)
            
            if polarity > 0.1:
                label = "positive"
            elif polarity < -0.1:
                label = "negative"
            else:
                label = "neutral"
            
            return (polarity, subjectivity, label)
        
        df = df.withColumn("sentiment", 
                          analyze_sentiment(F.col("title"), 
                                          F.col("description"), 
                                          F.col("content")))
        
        df = df.withColumn("sentiment_polarity", F.col("sentiment.polarity"))
        df = df.withColumn("sentiment_subjectivity", F.col("sentiment.subjectivity"))
        df = df.withColumn("sentiment_label", F.col("sentiment.label"))
        df = df.drop("sentiment")
        
        # 3. Extract commodities (Spark UDF)
        @F.udf(ArrayType(StringType()))
        def extract_commodities(title, content):
            text = f"{title or ''} {content or ''}".lower()
            commodities = []
            keywords = ['gold', 'silver', 'copper', 'oil', 'crude', 'natural gas',
                       'wheat', 'corn', 'soybeans', 'coffee', 'sugar', 'cotton']
            
            for commodity in keywords:
                if commodity in text:
                    commodities.append(commodity)
            
            return commodities
        
        df = df.withColumn("mentioned_commodities", 
                          extract_commodities(F.col("title"), F.col("content")))
        
        # 4. Extract tickers (Spark UDF)
        @F.udf(ArrayType(StringType()))
        def extract_tickers(title, content):
            text = f"{title or ''} {content or ''}"
            ticker_pattern = r'\b[A-Z]{2,5}\b'
            tickers = re.findall(ticker_pattern, text)
            
            stop_words = {'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL'}
            tickers = [t for t in tickers if t not in stop_words]
            
            # Return unique tickers
            return list(set(tickers))
        
        df = df.withColumn("mentioned_tickers", 
                          extract_tickers(F.col("title"), F.col("content")))
        
        # 5. Calculate impact score
        df = df.withColumn("commodity_count", F.size(F.col("mentioned_commodities")))
        df = df.withColumn("ticker_count", F.size(F.col("mentioned_tickers")))
        
        df = df.withColumn("impact_score", 
            F.lit(0.7) +  # Base score for newsapi
            (F.abs(F.col("sentiment_polarity")) * 2) +
            (F.col("commodity_count") * 0.3) +
            (F.col("ticker_count") * 0.1)
        )
        
        # 6. Add time features
        df = df.withColumn("publish_timestamp", 
                          F.to_timestamp(F.col("published_at")))
        df = df.withColumn("publish_date", 
                          F.to_date(F.col("published_at")))
        df = df.withColumn("publish_hour", 
                          F.hour(F.col("publish_timestamp")))
        df = df.withColumn("publish_day_of_week", 
                          F.dayofweek(F.col("publish_timestamp")))
        df = df.withColumn("is_market_hours", 
                          F.when((F.col("publish_hour") >= 9) & 
                                (F.col("publish_hour") <= 16), True)
                           .otherwise(False))
        
        # 7. Add processing metadata
        df = df.withColumn("transformation_timestamp", F.current_timestamp())
        df = df.withColumn("transformation_date", F.current_date())
        
        print(f"✅ Transformed {df.count()} articles")
        
        return df
    
    def save_to_silver(self, df):
        """Save transformed data to Silver layer"""
        
        # Write to Delta Lake (Silver)
        df_silver = (df.write
                        .format("delta")
                        .mode("append")
                        .partitionBy("transformation_date", "topic", "sentiment_label")
                        .option("mergeSchema", "true")
                        .saveAsTable(f"{CATALOG}.{SCHEMA}.slv_news_data"))
        
        print(f"✅ Saved to Silver: {f"{CATALOG}.{SCHEMA}.slv_news_data"}")
    
        
        # Optimize table
        spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.slv_news_data")
        
        return df_silver

# COMMAND ----------

# Run transformation
transformer = NewsTransformer()
silver_df = transformer.transform()
transformer.save_to_silver(silver_df)

display(silver_df)

# COMMAND ----------

