#%%
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from azure.storage.blob import container_client
import config
import os
spark = SparkSession.builder.config('spark.jars', f"{config.AZURE_STORAGE_JAR_PATH},{config.HADOOP_AZURE_JAR_PATH},{config.JSON_CON},{config.UTIL}").master('local').appName('ingestion').getOrCreate()


def applyLatest(df, type):
    #trades
    if type == "T":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "arrival_tm", "event_seq_nb").agg(max("arrival_tm").alias("latest_trade"))

        #Renaming the column names of the tables to avoid errors.
        df_grouped_r = df_grouped.select(*(col(x).alias(x + '_df_grouped') for x in df_grouped.columns))
        df_r = df.select(*(col(x).alias(x + '_df') for x in df.columns))

        final = df_grouped_r.join((df_r), (df_r.event_seq_nb_df ==df_grouped_r.event_seq_nb_df_grouped)  & (df_r.symbol_df ==df_grouped_r.symbol_df_grouped)  & (df_r.trade_dt_df ==df_grouped_r.trade_dt_df_grouped) &(df_r.arrival_tm_df == df_grouped_r.latest_trade_df_grouped) )
        # final = final.select(df_grouped_r["*_df_grouped"])
        # final_renamed = final.withColumnRenamed("trade_dt_df_grouped","Id").withColumnRenamed("college","College_Name")
        final = final.select('trade_dt_df_grouped', 'rec_type_df_grouped', 'symbol_df_grouped', 'arrival_tm_df_grouped', 'event_seq_nb_df_grouped', 'latest_trade_df_grouped', 'trade_pr_df','exchange_df','event_tm_df' )
        final_renamed = final.withColumnRenamed('trade_dt_df_grouped','trade_dt').withColumnRenamed('rec_type_df_grouped','rec_type').withColumnRenamed('symbol_df_grouped','symbol').withColumnRenamed('arrival_tm_df_grouped','arrival_tm').withColumnRenamed('event_seq_nb_df_grouped','event_seq_nb').withColumnRenamed('latest_trade_df_grouped','latest_trade').withColumnRenamed('trade_pr_df','trade_pr').withColumnRenamed('exchange_df','exchange').withColumnRenamed('event_tm_df','event_tm')
        return final_renamed
    #quotes
    elif type == "Q":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "event_seq_nb").agg(max("arrival_tm").alias("latest_quote"))

        #Renaming the column names of the tables to avoid errors.
        df_grouped_r = df_grouped.select(*(col(x).alias(x + '_df_grouped') for x in df_grouped.columns))
        df_r = df.select(*(col(x).alias(x + '_df') for x in df.columns))
        
        final = df_grouped_r.join(df_r, (df_r.event_seq_nb_df ==df_grouped_r.event_seq_nb_df_grouped)  & (df_r.symbol_df ==df_grouped_r.symbol_df_grouped)  & (df_r.trade_dt_df ==df_grouped_r.trade_dt_df_grouped) &(df_r.arrival_tm_df == df_grouped_r.latest_quote_df_grouped))
        final = final.select('trade_dt_df_grouped', 'rec_type_df_grouped' ,'symbol_df_grouped', 'event_seq_nb_df_grouped', 'latest_quote_df_grouped','bid_pr_df', 'bid_size_df', 'ask_pr_df','ask_size_df','exchange_df', 'event_tm_df')
        final_renamed = final.withColumnRenamed('trade_dt_df_grouped','trade_dt').withColumnRenamed('rec_type_df_grouped','rec_type').withColumnRenamed('symbol_df_grouped','symbol').withColumnRenamed('event_seq_nb_df_grouped','event_seq_nb').withColumnRenamed('latest_quote_df_grouped','latest_quote').withColumnRenamed('bid_pr_df','bid_pr').withColumnRenamed('bid_size_df','bid_size').withColumnRenamed('ask_pr_df','ask_pr').withColumnRenamed('ask_size_df','ask_size').withColumnRenamed('exchange_df','exchange').withColumnRenamed('event_tm_df','event_tm')
        return final_renamed

#Read and select data
# trade_common = spark.read.option('header',True).parquet('data/input/partition=T/*')

trade_common = spark.read.option('header',True).parquet("wasbs://{}@{}.blob.core.windows.net/{}".format(config.container_name,config.storage_account_name, 'step2_output/partition=T/*.parquet'))
trade = trade_common.select('rec_type', 'trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'trade_pr')
# quote_common = spark.read.option('header',True).parquet("data/input/partition=Q/  *")
quote_common = spark.read.option('header',True).parquet("wasbs://{}@{}.blob.core.windows.net/{}".format(config.container_name,config.storage_account_name, 'step2_output/partition=Q/*.parquet'))

quote = quote_common.select ("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

trade_corrected = applyLatest(trade, 'T')
quote_corrected = applyLatest(quote, 'Q')

trades_080520 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-05")
trades_080620 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-06")
quote_080520 = quote_corrected.where(quote_corrected.trade_dt == "2020-08-05")
quote_080620 = quote_corrected.where(quote_corrected.trade_dt == "2020-08-06")

#  Write The Trade Dataset 

trades_080520.write.mode("overwrite").parquet('data/output/trade/trade_dt={}'.format('2020-08-05'))
trades_080620.write.mode("overwrite").parquet('data/output/trade/trade_dt={}'.format('2020-08-06'))
quote_080520.write.mode("overwrite").parquet('data/output/quote/trade_dt={}'.format('2020-08-05'))
quote_080620.write.mode("overwrite").parquet('data/output/quote/trade_dt={}'.format('2020-08-06'))

# Upload data to Azure Blob Storage
container_client
blob_service = container_client(config.your_connection_string, config.your_container_name)

path_remove = "/Users/daniel/Desktop/Data_Engineering/Guided Capstone/Step 3 End-of-Day (EOD) Data Load/data/"
local_path = "/Users/daniel/Desktop/Data_Engineering/Guided Capstone/Step 3 End-of-Day (EOD) Data Load/data/output"

for r,d,f in os.walk(local_path):        
    if f:
        for file in f:
            file_path_on_azure = os.path.join(r,file).replace(path_remove,"")
            file_path_on_local = os.path.join(r,file)
            blob_service.create_blob_from_path(config.container_name,file_path_on_azure,file_path_on_local)            

