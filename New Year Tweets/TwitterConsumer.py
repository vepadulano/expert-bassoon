from kafka import KafkaConsumer
import json
consumer = KafkaConsumer(bootstrap_servers='sandbox.hortonworks.com:6667', auto_offset_reset='earliest', consumer_timeout_ms=1000)
consumer.subscribe(['newyear'])
with open("NewYearTweets.json","a") as f:
    while True:
        for message in consumer:
            data = json.loads(message.value)
            f.write('\n')
            json.dump(data,f)
f.close()
