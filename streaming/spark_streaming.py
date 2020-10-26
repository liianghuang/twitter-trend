from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, StreamingListener
from pyspark.streaming.kafka import KafkaUtils  
from cassandra.cluster import Cluster
import cassandra
from nltk.corpus import stopwords
from collections import Counter, defaultdict
from math import log
import json 
import string
import re
import os.path

"""
spark-submit --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.3,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.1 spark_streaming.py

# optimization
spark.streaming.kafka.consumer.cache.enabled=false
"""

class Listener(StreamingListener):
    def __init__(self, sparkcontext):
        self.sc = sparkcontext

    def onBatchCompleted(self, batchCompleted):
        print(batchCompleted.toString())

        # update the count of tweets in every subreddits
        with open("../config.json", "r") as f: config = json.load(f)
        cluster = Cluster([config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
        session = cluster.connect(config["CASSANDRA"]["KEYSPACE"])
        tweet_count_accumulator_dict = get_tweet_count_dict(self.sc)
        tweet_count_dict = {k: v.value for k, v in tweet_count_accumulator_dict.items()}

        # for web        
        rank = Counter(tweet_count_dict)
        top_tweets = rank.most_common(config["WEB"]["NUM_SUBREDDIT"])
        query = "INSERT INTO others (category, content) \
            VALUES ('tweet_count_web', '%s')" % (json.dumps(dict(top_tweets)))
        session.execute(query)
        session.shutdown()
        return

        class Java:
            implements = ["org.apache.spark.streaming.api.java.PythonStreamingListener"]

def get_tweet_count_dict(sparkContext):
    if ('tweet_count_dict' not in globals()):
        with open("../config.json", "r") as f: config = json.load(f)
        cluster = Cluster([config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
        session = cluster.connect(config["CASSANDRA"]["KEYSPACE"])
        query = "SELECT subreddit FROM %s" % \
			config["CASSANDRA"]["WORD_COUNT_TABLE"]
        response = session.execute(query)

        tweet_count_dict = {}
        for row in response:
            tweet_count_dict[row.subreddit] = sparkContext.accumulator(0)

        session.shutdown()

        globals()['tweet_count_dict'] = tweet_count_dict

    return globals()['tweet_count_dict']

class twitterStreamingProcess(object):
    def __init__( self ):
        with open(os.path.dirname(__file__) + "/../config.json", "r") as f:
            self.config = json.load(f)
        self.english_stopwords = stopwords.words("english")
        self.num_subreddit = None
        self.subreddit_word_count_dict = self.get_subreddit_word_count()

    def get_top_topic(self, word_set):
        if not word_set:
            return "No matched reddit"
		
        word_freq_dict = defaultdict(float)
		
		# query cassandra to get the subreddits which have the words
        cluster = Cluster([self.config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
        session = cluster.connect(self.config["CASSANDRA"]["KEYSPACE"])
        query = "SELECT counts FROM %s WHERE word IN ('" % (self.config["CASSANDRA"]["WORD_FREQUENCY_TABLE"]) + "', '".join(word_set) + "')"
        response = session.execute(query)

		# use tf-itf to rank the subreddits
        for row in response:
            freq_dict = json.loads(row.counts)
            for reddit in freq_dict:
			    # using tf-itf
                word_freq_dict[reddit] += (freq_dict[reddit] * \
						log(self.num_subreddit/float(len(freq_dict))))
        session.shutdown()

        if not word_freq_dict:
            return "No matched"
        else:
            for result, _ in Counter(word_freq_dict).most_common(self.config["TWITTER_STREAMING"]["SEARCH_TOP_SUBREDDIT_THRESHOULD"]):
                if self.subreddit_word_count_dict[result] > self.config\
					["TWITTER_STREAMING"]["WORD_COUNT_THRESHOULD"]:
                    tweet_count_dict[result].add(1)
                    return result
            return "No match the threshould constraints"

    def get_word_set(self, tweet_text, user):
        tweet_text = filter(lambda x: x in set(string.printable), tweet_text)
        tweet_text = "".join(tweet_text)
        tweet_text = tweet_text.lower()
        word_list = re.findall(r'\w+', tweet_text,flags = re.UNICODE)
        important_word_list = filter(lambda x: x not in self.english_stopwords, word_list)
        return set(important_word_list)

    def get_subreddit_word_count(self):
        cluster = Cluster([self.config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
        session = cluster.connect(self.config["CASSANDRA"]["KEYSPACE"])
        query = "SELECT * FROM %s" % (self.config["CASSANDRA"]["WORD_COUNT_TABLE"])
        response = session.execute(query)
        
        subreddit_word_count_dict = {}
        for row in response:
            subreddit_word_count_dict[row.subreddit] = row.word_count

        return subreddit_word_count_dict

    def start(self):
        # optimize spark-streaming performance
        conf = SparkConf()
        conf.set("spark.locality.wait", 10)
        conf.set("spark.streaming.backpressure.enabled", True)
        conf.set("spark.streaming.kafka.consumer.poll.ms", 512)
        conf.set("spark.streaming.receiver.maxRate", 1000)
        sc = SparkContext(conf=conf, appName = "spark_streaming_kafka")
        
        sc.setLogLevel("WARN")
        ssc = StreamingContext(sc, \
				self.config["TWITTER_STREAMING"]["MINI_BATCH_TIME_INTERVAL_SEC"] )
        listener = Listener(sc)
        ssc.addStreamingListener(listener)

        self.num_subreddit = len(get_tweet_count_dict(sc))
        print("classify to %d" % \
				self.num_subreddit)

        # union of streams
        numStreams = 8
        kafkaStreams = [KafkaUtils.createStream(ssc, self.config["DEFAULT"]\
				["KAFKA_PUBLIC_IP"]+':2181', 'spark-streaming', {'twitter':1}) for _ in range (numStreams)]
        unifiedStream = ssc.union(*kafkaStreams) 

        # if used direct stream
        #kafkaStream = KafkaUtils.createDirectStream(ssc, ['twitter'], {"metadata.broker.list": self.config["DEFAULT"]["KAFKA_BROKER_LIST"]})
        
		
		# load streaming message from kafka
        parsed = unifiedStream.map(lambda v: json.loads(v[1]))
        
        # debug usage
        #parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

		# process and classify tweets
        subreddit_topic = parsed\
                #.map(lambda tweet: self.get_word_set(tweet['text'], tweet['user']['name']))
                .map(lambda tweets: (tweet['user']['name'], self.get_word_set(tweet['text']))
        
        subreddit_topic = subreddit_topic.map(self.get_top_topic)
        subreddit_topic.pprint()
		
        ssc.start()
        ssc.awaitTermination()
        return

def main():
    process = twitterStreamingProcess()
    process.start()

if __name__ == '__main__':
    main()