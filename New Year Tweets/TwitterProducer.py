from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient, KafkaProducer

access_token = "702070782885761024-tDsRbghpFaMVW4j634aP9RsgpCz2CXN"
access_token_secret =  "OvpJdFQl3CoNlWHSGY8kRyjNbWfmboAjEOI1v3j6stYYB"
consumer_key =  "pb6JVk0ckTblm1BomRrQv7XGc"
consumer_secret =  "hqmEBCHBJqd6NpvNb1omWHrbsqYKEPNlGyJIsDh21u3rdYfNLx"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("newyear", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

producer = KafkaProducer(bootstrap_servers='sandbox.hortonworks.com:6667')
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["newyearresolution","newyearsresolution"],languages=["en"])
