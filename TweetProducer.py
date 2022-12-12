from kafka import KafkaProducer
import tweepy
import datetime
import time
import json
from geopy.geocoders import Nominatim
from geotext import GeoText
from textblob import TextBlob


def tweeting():
    geolocator = Nominatim(user_agent = "geoapiExercises")

    client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAACZhkAEAAAAAFI0fNL%2BEZoVtoxX98BqM9HcUDZ0%3DDY3TVFCH2Gokl0IufCOMFrPUcTEJEZh0ntiu1AduQVDmXIJnTk')
    producer = KafkaProducer()
    # producer.flush()
    query = 'worldcup'
    start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
    end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)

    while True:
        tweets = client.search_recent_tweets(query=query,
                                             tweet_fields=["attachments",
                                                           "author_id",
                                                           "context_annotations",
                                                           "conversation_id",
                                                           "created_at",
                                                           "edit_history_tweet_ids",
                                                           "entities",
                                                           "geo",
                                                           "id",
                                                           "in_reply_to_user_id",
                                                           "lang",
                                                           "possibly_sensitive",
                                                           "public_metrics",
                                                           "referenced_tweets",
                                                           "reply_settings",
                                                           "source",
                                                           "text",
                                                           "withheld"],
                                             user_fields=["created_at",
                                                          "description",
                                                          "entities",
                                                          "id",
                                                          "location",
                                                          "name",
                                                          "pinned_tweet_id",
                                                          "profile_image_url",
                                                          "protected",
                                                          "public_metrics",
                                                          "url",
                                                          "username",
                                                          "verified",
                                                          "withheld"],
                                             place_fields=["contained_within",
                                                           "country",
                                                           "country_code",
                                                           "full_name",
                                                           "geo",
                                                           "id",
                                                           "name",
                                                           "place_type"],
                                             expansions=[
                                                 "author_id",
                                                 "edit_history_tweet_ids",
                                                 "entities.mentions.username",
                                                 "geo.place_id",
                                                 "in_reply_to_user_id",
                                                 "referenced_tweets.id",
                                                 "referenced_tweets.id.author_id"
                                             ],
                                             max_results=100,
                                             start_time=start_time,
                                             end_time=end_time
                                             )
        # print(tweets)
        start_time = end_time
        end_time = start_time + datetime.timedelta(seconds=10)

        for i, tweet in enumerate(tweets.data):
            dico_dico = {}
            users = tweets.includes["users"]
            for i in range(len(users)):
                if users[i]["id"] == tweet.author_id:
                    name = users[i]["name"]
                    username = users[i]["username"]
                    location = users[i].location
                    break

            # print(location)
            if location != None:
                Real_location = geolocator.geocode(location)
                if Real_location != None:
                    last = Real_location.raw['display_name'].rsplit(',', 1)[-1].replace("\\s", "")
                    places = GeoText(last)
                    if places.countries != []:
                        location = places.countries[0]
                    else:
                        location = None
                else:
                    location = None
            if location != None:
                analysis = TextBlob(tweet.text)
                analysis = analysis.sentiment
                polarity = analysis.polarity
                sentiment = "neutral"
                if polarity > 0:
                    sentiment = "positive"
                elif polarity < 0:
                    sentiment = "negative"

                dico_tmp = {"id": tweet.id, "user": username + name, "lang": tweet.lang, "created_at": tweet.created_at.strftime("%m/%d/%Y, %H:%M"),
                            "text": tweet.text, "location": location, "sentiment": sentiment}
                print(dico_tmp)
                # dico_dico[i] = dico_tmp
                if tweet.lang == 'en':
                    tweet_to_json = json.dumps(dico_tmp, indent = 4).encode('utf-8')
                    # tweet = json.dumps(tweet.text).encode('utf-8')
                    producer.send('worldcup', tweet_to_json)
                print(f'Le {i}ème tweet a été envoyé à Kafka avec succès!')
        #     # print(dico_dico)
        print('Pause de 10 secondes!')
        time.sleep(10)

tweeting()