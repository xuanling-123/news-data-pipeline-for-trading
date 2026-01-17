# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import re

# COMMAND ----------

CATALOG = "coe_dev_dbricks_poc_usecases"
SCHEMA = "poc_databricks_explore"
SILVER_PATH = f"{CATALOG}.{SCHEMA}.slv_news_data"

# COMMAND ----------

class NewsAggregator:
    """Create Gold layer aggregations"""
    
    def create_daily_sentiment_summary(self, silver_path=SILVER_PATH):
        """Daily sentiment by topic"""
        
        df = spark.table(silver_path)
        
        summary_df = (df
            .groupBy("publish_date", "topic")
            .agg(
                F.avg("sentiment_polarity").alias("avg_sentiment"),
                F.count("*").alias("article_count"),
                F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
                F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
                F.sum(F.when(F.col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_count"),
                F.avg("impact_score").alias("avg_impact_score"),
                F.max("impact_score").alias("max_impact_score")
            )
            .orderBy("publish_date", "topic")
        )
        
        # Save to Gold
        (summary_df.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(f"{CATALOG}.{SCHEMA}.gld_news_daily_sentiment"))
        
        print("✅ Created daily sentiment summary")
        return summary_df
    
    def create_commodity_analysis(self, silver_path=SILVER_PATH):
        """Commodity mention analysis"""
        
        df = spark.table(silver_path)
        
        # Explode commodities array
        commodity_df = (df
            .select("publish_date", "topic", 
                   F.explode("mentioned_commodities").alias("commodity"),
                   "sentiment_polarity", "impact_score")
            .groupBy("commodity", "publish_date")
            .agg(
                F.count("*").alias("mention_count"),
                F.avg("sentiment_polarity").alias("avg_sentiment"),
                F.avg("impact_score").alias("avg_impact")
            )
            .orderBy(F.desc("mention_count"))
        )
        
        # Save to Gold
        (commodity_df.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(f"{CATALOG}.{SCHEMA}.gld_news_commodity_analysis"))
        
        print("✅ Created commodity analysis")
        return commodity_df
    
    def create_high_impact_articles(self, silver_path=SILVER_PATH, top_n=100):
        """Top impact articles"""
        
        df = spark.table(silver_path)
        
        high_impact_df = (df
            .select("title", "url", "topic", "sentiment_label", 
                   "sentiment_polarity", "impact_score", 
                   "mentioned_commodities", "mentioned_tickers",
                   "publish_date", "publish_timestamp")
            .orderBy(F.desc("impact_score"))
            .limit(top_n)
        )

        high_impact_df = high_impact_df.dropDuplicates(["title", "publish_date"])
        
        # Save to Gold
        (high_impact_df.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(f"{CATALOG}.{SCHEMA}.gld_news_high_impact_articles"))
        
        print("✅ Created high impact articles")
        return high_impact_df

# Create Gold tables
aggregator = NewsAggregator()
daily_sentiment = aggregator.create_daily_sentiment_summary()
commodity_analysis = aggregator.create_commodity_analysis()
high_impact = aggregator.create_high_impact_articles()

display(high_impact)

# COMMAND ----------

