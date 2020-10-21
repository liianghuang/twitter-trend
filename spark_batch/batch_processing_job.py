# pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType
from nltk.corpus import stopwords
# cassandra
from cassandra.cluster import Cluster
import cassandra
# python
from collections import Counter, defaultdict
import os.path
import os
import json
import re
"""
This code produce the tables needed to classify tweets and save to cassandra
"""
"""
To run this code:
spark-submit batch_processing.py --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \
                                 --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
                                 --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
                                 --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider
                                 #-–conf spark.dynamicAllocation.enabled=false
                                 #--conf fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem

# tested with spark-shell
nohup spark-submit --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeaazonaws.com --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider --packages "org.apache.hadoop:hadoop-aws:2.7.3,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3" batch_processing.py &
"""

class RedditBatchProcess(object):
    def __init__( self ):
        self.english_stopwords = stopwords.words("english")
        self.total_word_count_dict = {}
        with open(os.path.dirname(__file__) + "/../config_new.json", "r") as f:
            self.config = json.load(f)

    def word_process(self, text):
        text_str = text.lower()
        word_list = re.findall(r'\w+', text_str)
        important_word_list = list(filter(lambda x: x not in \
                self.english_stopwords, word_list))
        return important_word_list

    # major bottel-neck
    def generate_word_count(self, row):
        word_counter = Counter(row[1])
        return [ (word, [ (row[0], word_counter[word]) ] ) \
                for word in word_counter ]

    def construct_subreddit_word_freq(self, row):
        word, subreddit_word_count_list = row

        subreddit_counter = Counter()
        for subreddit, word_count in subreddit_word_count_list:
                subreddit_counter[subreddit] += word_count
            
        subreddit_dict = dict( {k: v/float(self.total_word_count_dict[k]) \
                if self.total_word_count_dict[k] != 0 else 0 \
                for k, v in subreddit_counter.most_common()} )
        return (word, json.dumps(subreddit_dict))

    def start(self):
        """ start batch processing
        """
        sc = SparkContext(appName="spark_batch_process")


        hadoop_conf = sc._jsc.hadoopConfiguration()
        
        # configs to solve s3 V4 API issue with ap-northeast-2
        hadoop_conf.set("fs.s3a.awsAccessKeyId", "yourkey")
        hadoop_conf.set("fs.s3a.awsSecretAccessKey", "yourkey")
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.setLogLevel("WARN")
        hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf.set("fs.s3a.endpoint", "s3-ap-northeast-2.amazonaws.com")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.InstanceProfileCredentialsProvider")
        hadoop_conf.set("spark.hadoop.fs.s3a.access.key", "yourkey")
        hadoop_conf.set("spark.hadoop.fs.s3a.secret.key", "yourkey")
        hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
<<<<<<< HEAD:spark_batch/batch_processing_job.py
        
=======
        #hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem") --s3n
>>>>>>> parent of 5ac7036... clean up batch script:spark_batch/batch_processing.py
        sqlContext = SQLContext(sc)

        # load data from S3
        #df = sqlContext.read.option("mode", "DROPMALFORMED").option("compression", "org.apache.spark.io.ZStdCompressionCodec").json(self.config["REDDIT_BATCH_PROCESS"]["S3_DATA_PATH"])
        for i in range(len(self.config["REDDIT_BATCH_PROCESS"]["S3_DATA_PATH_LIST"])):
            if i == 0:
                df = sqlContext.read.option("mode", "DROPMALFORMED").option("compression", "org.apache.spark.io.ZStdCompressionCodec").json(self.config["REDDIT_BATCH_PROCESS"]["S3_DATA_PATH_LIST"][i])    
            else:
                df_temp = sqlContext.read.option("mode", "DROPMALFORMED").option("compression", "org.apache.spark.io.ZStdCompressionCodec").json(self.config["REDDIT_BATCH_PROCESS"]["S3_DATA_PATH_LIST"][i])
                df = df.union(df_temp)
        #.option("compression", "org.apache.spark.io.ZStdCompressionCodec")
        #.\
                #option("compression", "org.apache.spark.io.ZStdCompressionCodec")
                
        df = df.select(["subreddit", "body"])
        cluster = Cluster([self.config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
        session = cluster.connect('reddit')
        freq_table_name = "word_freq_table"
        count_table_name = "word_count_table"

        # create tables if not exists
        # try:
        #     query = "CREATE TABLE %s (word text, counts text,\
        #             PRIMARY KEY (word), )" % \
        #             freq_table_name
        #     response = session.execute(query)
        # except cassandra.AlreadyExists:
        #     # query = "TRUNCATE %s" % \
        #     #         freq_table_name
        #     # response = session.execute(query)
        # try:
        #     query = "CREATE TABLE %s (subreddit text, word_count counter, \
        #             PRIMARY KEY (subreddit), )" % \
        #             count_table_name
        #     response = session.execute(query)
        # except cassandra.AlreadyExists:
            # query = "TRUNCATE %s" %\
            #         count_table_name
            # response = session.execute(query)

        subreddit_word_rdd = df.rdd.\
                map(lambda row: (row.subreddit, self.word_process(row.body)))
        subreddit_word_count_df = \
                subreddit_word_rdd.map(lambda row: (row[0], len(row[1])))\
            .reduceByKey( lambda a, b: a + b ).toDF(["subreddit", "word_count"])
        
        # subreddit_word_count_df.show()
        
        # subreddit_word_count_df.write\
        #     .format("org.apache.spark.sql.cassandra")\
        #     .mode('append')\
        #     .options(table=count_table_name,
        #         keyspace='reddit')\
        #     .save()

        query = "SELECT * FROM %s" % \
                count_table_name
        response = session.execute(query)

        for row in response:
            self.total_word_count_dict[row.subreddit] = row.word_count

        session.shutdown()

        group_word_rdd = subreddit_word_rdd\
                .flatMap( self.generate_word_count )\
            .reduceByKey(lambda a, b: a + b)

        group_subreddit_df = group_word_rdd\
                .map(self.construct_subreddit_word_freq).toDF(["word", "counts"])
        
        group_subreddit_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=freq_table_name, \
                keyspace='reddit')\
            .save()

        return

class TwitterBatchProcessing(object):
    def __init__( self ):
        self.english_stopwords = stopwords.words("english")
        self.total_word_count_dict = {}
        with open(os.path.dirname(__file__) + "/../config_new.json", "r") as f:
            self.config = json.load(f)

    def word_process(self, text):
        text_str = text.lower()
        word_list = re.findall(r'\w+', text_str)
        important_word_list = list(filter(lambda x: x not in \
                self.english_stopwords, word_list))
        return important_word_list

    def generate_word_count(self, row):
        word_counter = Counter(row[1])
        return [ (word, [ (row[0], word_counter[word]) ] ) \
                for word in word_counter ]

    def construct_subreddit_word_freq(self, row):
        word, subreddit_word_count_list = row

        subreddit_counter = Counter()
        for subreddit, word_count in subreddit_word_count_list:
                subreddit_counter[subreddit] += word_count
            
        subreddit_dict = dict( {k: v/float(self.total_word_count_dict[k]) \
                if self.total_word_count_dict[k] != 0 else 0 \
                for k, v in subreddit_counter.most_common()} )
        return (word, json.dumps(subreddit_dict))

    def start( self ):
        """ start batch processing
        """
        sc = SparkContext(appName="spark_batch_process")
        access_id = self.config['AWS']['ACCESS_ID']
        access_key = self.config['AWS']['ACCESS_KEY']
        aws_region = "ap-northeast-2"
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoopConf.set("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf.set("fs.s3a.access.key", access_id)
        hadoop_conf.set("fs.s3a.secret.key", access_key)

        # see https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
        hadoop_conf.set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")
        sc.setLogLevel("WARN")
        hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
        sqlContext = SQLContext(sc)

        # load data from S3
        df = sqlContext.read\
            .option("mode", "DROPMALFORMED").option("compression", "org.apache.spark.io.ZStdCompressionCodec").json(self.config["REDDIT_BATCH_PROCESS"]["S3_DATA_PATH"])
            #.option("compression", "org.apache.spark.io.ZStdCompressionCodec")\ #don't use it if using bz2 compression
            #.\
                #option("compression", "org.apache.spark.io.ZStdCompressionCodec")
                
        df = df.select(["subreddit", "body"])
        cluster = Cluster([self.config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
        session = cluster.connect('reddit')
        freq_table_name = "word_freq_table"
        count_table_name = "word_count_table"

        # create tables if not exists
        # try:
        #     query = "CREATE TABLE %s (word text, counts text,\
        #             PRIMARY KEY (word), )" % \
        #             freq_table_name
        #     response = session.execute(query)
        # except cassandra.AlreadyExists:
        #     # query = "TRUNCATE %s" % \
        #     #         freq_table_name
        #     # response = session.execute(query)

        # try:
        #     query = "CREATE TABLE %s (subreddit text, word_count counter, \
        #             PRIMARY KEY (subreddit), )" % \
        #             count_table_name
        #     response = session.execute(query)
        # except cassandra.AlreadyExists:
            # query = "TRUNCATE %s" %\
            #         count_table_name
            # response = session.execute(query)

        subreddit_word_rdd = df.rdd.\
                map(lambda row: (row.subreddit, self.word_process(row.body)))
        subreddit_word_count_df = \
                subreddit_word_rdd.map(lambda row: (row[0], len(row[1])))\
            .reduceByKey( lambda a, b: a + b ).toDF(["subreddit", "word_count"])

        subreddit_word_count_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=count_table_name,
                keyspace='reddit')\
            .save()

        query = "SELECT * FROM %s" % \
                count_table_name
        response = session.execute(query)

        for row in response:
            self.total_word_count_dict[row.subreddit] = row.word_count

        session.shutdown()

        group_word_rdd = subreddit_word_rdd\
                .flatMap( self.generate_word_count )\
            .reduceByKey(lambda a, b: a + b)

        group_subreddit_df = group_word_rdd\
                .map(self.construct_subreddit_word_freq).toDF(["word", "counts"])
        group_subreddit_df = group_subreddit_df.withColumn("counts", df["counts"].cast(DoubleType()))
        
        group_subreddit_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=freq_table_name, \
                keyspace='reddit')\
            .save()

        return

def main():
    process = RedditBatchProcess()
    process.start()
    return

if __name__ == '__main__':
    main()
