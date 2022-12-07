from kafka import KafkaProducer
import tweepy
import datetime
import time
import json

client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAACZhkAEAAAAAFI0fNL%2BEZoVtoxX98BqM9HcUDZ0%3DDY3TVFCH2Gokl0IufCOMFrPUcTEJEZh0ntiu1AduQVDmXIJnTk')
producer = KafkaProducer()
# producer.flush()
query = 'worldcup'
start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)

while True:
    tweets = client.search_recent_tweets(query=query,
                                        tweet_fields=['context_annotations', 'created_at', 'lang'],
                                        max_results=100,
                                        start_time=start_time,
                                        end_time=end_time)
    start_time = end_time
    end_time = start_time + datetime.timedelta(seconds=10)

    for i,tweet in enumerate(tweets.data):
        if tweet.lang == 'en':
            print(tweet)
            tweet = json.dumps(tweet.text).encode('utf-8')
            producer.send('worldcup', tweet)
        print(f'Le {i}ème tweet a été envoyé à Kafka avec succès!')
    print('Pause de 10 secondes!')
    time.sleep(10)