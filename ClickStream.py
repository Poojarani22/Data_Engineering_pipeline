from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch, helpers
import happybase

# Define Kafka connection properties
kafka_bootstrap_servers = "kafka_bootstrap_servers"
kafka_topic = "kafka_topic"

# Define HBase connection properties
hbase_host = "hbase_host"
hbase_port = 9090
hbase_table = "hbase_table"

# Define Elasticsearch connection properties
es_host = "localhost"
es_port = 9200
es_index = "elasticsearch_index"

# Create a SparkSession
spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

# Connect to HBase
connection = happybase.Connection('your_hbase_host')

# Define the schema for clickstream data
schema = StructType([
    StructField("row_key", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("browser", StringType(), nullable=True),
    StructField("operating_sys", StringType(), nullable=True),
    StructField("device", StringType(), nullable=True)
])

# Create the HBase table
table_name = 'clickstream_hbase_table'
column_families = {
    'click_data': {},
    'geo_data': {},
    'user_agent_data': {}
}
connection.create_table(table_name, column_families)

# Function to write click event data to HBase
def write_to_hbase(row):
    table = connection.table(table_name)
    click_event = row.asDict()
    row_key = click_event['row_key']
    user_id = click_event['user_id']
    timestamp = click_event['timestamp']
    url = click_event['url']
    country = click_event['country']
    city = click_event['city']
    browser = click_event['browser']
    os = click_event['os']
    device = click_event['device']
    table.put(row_key, {
        'click_data:user_id': user_id,
        'click_data:timestamp': timestamp,
        'click_data:url': url,
        'geo_data:country': country,
        'geo_data:city': city,
        'user_agent_data:browser': browser,
        'user_agent_data:os': os,
        'user_agent_data:device': device
    })

# Read data from Kafka stream
kafkaDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "your_kafka_bootstrap_servers") \
    .option("subscribe", "your_kafka_topic") \
    .load()

# Parse the JSON click event data
parsedDF = kafkaDF.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("click_event")) \
    .select("click_event.*")

# Write the parsed data to HBase
query = parsedDF.writeStream \
    .foreach(write_to_hbase) \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()


	
# Read the clickstream data from HBase into a Df
df = spark.read \
    .format("org.apache.spark.sql.execution.datasources.hbase") \
    .options(table=clickstream_hbase_table, host=hbase_host, port=str(hbase_port)) \
    .load()

aggregations = df.groupBy('click_data:url', 'geo_data:country') \
    .agg(
        countDistinct('click_data:user_id').alias('unique_users'),
        countDistinct('click_data:row_key').alias('clicks'),
        avg('click_data:timestamp').alias('avg_time_spent')
    )

# Show the result
aggregations.show()


## ELASTICSEARCH INDEXING ##


# Define the Elasticsearch mapping
es_mapping = {
    "properties": {
        "url": {"type": "keyword"},
        "country": {"type": "keyword"},
        "unique_users": {"type": "long"},
        "clicks": {"type": "long"},
        "avg_time_spent": {"type": "float"}
    }
}

# Create an Elasticsearch connection
es = Elasticsearch([{"host": es_host, "port": es_port}])

# Create Elasticsearch index with the specified mapping
es.indices.create(index=clickstream_data, body={"mappings": es_mapping})

# Convert DataFrame rows to Elasticsearch documents
documents = aggregations.rdd.map(lambda row: {
    "url": row.url,
    "country": row.country,
    "unique_users": row.unique_users,
    "clicks": row.clicks,
    "avg_time_spent": row.avg_time_spent
})

# Index the documents into Elasticsearch
helpers.bulk(es, documents.collect(), index=clickstream_data)

# Refresh the Elasticsearch index
es.indices.refresh(index=clickstream_data)