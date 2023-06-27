# Data_Engineering_pipeline
Data Engineering Assessment

We can use Apache Kafka for the data ingestion as we are dealing with streaming data and kafka is very fast and ensures  zero downtime. 
For the Data storage we can use Hbase(NoSQL database).
Processing can be done by spark, as spark can process huge data with high speed. We use pyspark to perform transformations as required.
ElasticSearch for data indexing.

The explaination of the working of code:

We can write the code in Databricks, as it provides a managed Spark environment that allows you to easily execute Spark code and work with various data sources. We need to ensure all the necessary dependencies are installed.


We first create a connection to Hbase, manually specify schema for the data,create Hbase Table.
We then read the data from Kafka stream using Spark.readStream and consume from kafka topic specified. 
Parse the json data by providing the schema and then load the parsed data to Hbase table and we wait for the streaming query to finish using awaitTermination().
Then we Read the clickstream data from HBase into a Df and perform the given transformation.

Elasticsearch

Add hostname and port of  Elasticsearch cluster. We add the elasticsearch mapping having the types of fields.
We create a new index named clickstream_data and pass the mapping we created for the fields. Then we convert our dataframe rows to elasticsearch documents and the documents are indexed into Elasticsearch using helpers.bulk() function (utility provided by the Elasticsearch Python client library), and the Elasticsearch index is refreshed using es.indices.refresh().
