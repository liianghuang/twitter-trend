from flask import render_template
from app import app
import os.path
import json
from cassandra.cluster import Cluster
from collections import Counter


with open("config.json", "r") as f:
    config = json.load(f)

cluster = Cluster([config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
session = cluster.connect(config["CASSANDRA"]["KEYSPACE"])

# demo web page
@app.route("/demo")
def demo():
 return render_template("bar.html")

# update graph
@app.route('/update', methods=['GET'])
def update():
    query = "SELECT content FROM others WHERE category = 'tweet_count_web'"
    response = session.execute(query)
    return json.dumps(dict(Counter(json.loads(response[0].content)).most_common()[1:]))