# coding: utf-8
import tweepy
import kafka
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

consumer_key = "2NWeA37a9tny7S7klT6jGhWDZ5v"
consumer_secret = "aL7QS80ECR4bKniBPnnDfDYklW2mb4rlsoSi8Nb5KtDjfZU0idiE"
access_key = "2795049142-TdE4d9OwtPBkldd3V1rGHrmtPz9UocG8x2aGT1YHu"
access_secret = "L0ygY1KY0NxePWhfok70PDGJCklVvjM6nMOTeIwh3gLBqBo"

#creating an OAuthHandler instance
auth=tweepy.OAuthHandler(consumer_key,consumer_secret)

#The access token is the “key” for opening the Twitter API treasure box
auth.set_access_token(access_key,access_secret)
api = tweepy.API(auth)

#Creating a Kafka Producer Instance
myproducer=KafkaProducer(bootstrap_servers='sandbox.hortonworks.com:6667',acks=1,retries=1)

print("List of Partitions for topic 'ktwitter': "+str(myproducer.partitions_for('ktwitter')))

with open('twitterhandle.txt','r+') as f:
   for handle in f:
     
     print("Getting Tweets from "+str(handle).strip())
   
     alltweets = []
     tweets=api.user_timeline(screen_name=handle,count=10)
     alltweets.extend(tweets)
   
     for x in range(len(alltweets)):
        #print(alltweets[x].id, alltweets[x].lang,alltweets[x].source,alltweets[x].retweeted,type(alltweets[x].text),alltweets[x].text.encode('utf-8'))
        try:
           future=myproducer.send('ktwitter',value=alltweets[x].text.encode('utf-8'))
        except KafkaTimeoutError as err:
          print("KafkaTimeoutError Encountered While Sending...: "+err)

     try:
	    #Flush is blocking until all messages or atleast put on the network , it does not guarantee delivery or success
        myproducer.flush()
     except KafkaTimeoutError as err:
      print("KafkaTimeoutError Encountered While Sending...: "+err)
      
     
	 #Get() method is blocking until message is sent or timedout, Returns a recordmetadata object which contains details like topic, partition written to and the offset
	 #For a Nonblocking send and still to get partition and offset details , use future.values variable
     return_Record_Metadata=future.get()
     
     if future.is_done:
         print("Write successful to topic: "+return_Record_Metadata.topic+" partition: "+str(return_Record_Metadata.partition)+" Offset:"+str(return_Record_Metadata.offset)+"\n")
     else:
         print("Write Failed to topic: "+return_Record_Metadata.topic+"partitions: "+str(return_Record_Metadata.partition))

myproducer.close()
