# coding: utf-8
import tweepy
import kafka
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

consumer_key = "2NWeA37a9tny7S7T6jGhWDZ5v"
consumer_secret = "aL7QS80ECR4bKniBPnnDfDYW2mb4rlsoSi8Nb5KtDjfZU0idiE"
access_key = "2795049142-TdE4d9OwtPBdd3V1rGHrmtPz9UocG8x2aGT1YHu"
access_secret = "L0ygY1KY0NxePWhfok70PDGJCVvjM6nMOTeIwh3gLBqBo"

#creating an OAuthHandler instance
auth=tweepy.OAuthHandler(consumer_key,consumer_secret)

#The access token is the “key” for opening the Twitter API treasure box
auth.set_access_token(access_key,access_secret)
api = tweepy.API(auth)

myproducer=KafkaProducer(bootstrap_servers='sandbox.hortonworks.com:6667',acks=1,retries=1)

with open('twitterhandle.txt','r+') as f:
   for handle in f:
     
     print("Getting Tweets from "+str(handle).strip())
   
     alltweets = []
     tweets=api.user_timeline(screen_name=handle,count=20)
     alltweets.extend(tweets)
   
     for x in range(len(alltweets)):
        #print(alltweets[x].id, alltweets[x].lang,alltweets[x].source,alltweets[x].retweeted,type(alltweets[x].text),alltweets[x].text.encode('utf-8'))
        try:
           future=myproducer.send('ktwitter',value=alltweets[x].text.encode('utf-8'))
        except KafkaTimeoutError as err:
          print("KafkaTimeoutError Encountered While Sending...: "+err)

     try:
        myproducer.flush()
     except KafkaTimeoutError as err:
      print("KafkaTimeoutError Encountered While Sending...: "+err)
      
     #print("Alltweets from "+ str(handle).strip()+" flushed")
     
     if future.is_done:
         print("Write successful to topic: "+future.get().topic+" partition: "+str(future.get().partition)+"\n")
     else:
         print("Write Failed to topic: "+future.get().topic+"partitions: "+str(future.get().partition))

myproducer.close()
