# coding: utf-8
from kafka import KafkaConsumer
from kafka import TopicPartition
myconsumer = KafkaConsumer(group_id='ktwitter_group',bootstrap_servers='sandbox.hortonworks.com:6667',auto_offset_reset='earliest',consumer_timeout_ms=1000)
#consumer_timeout_ms=number of milliseconds to block during message iteration(below) before raising StopIteration, Default block forever [float(‘inf’)]. if not set , consumer will not come out of the iteration
#If topic name is mentioned,It internally calls subscribe method or we can use the assign method

positive=['DEVELOPMENT','PROGRESS','GROWTH','POSITIVE','WISHING','CONGRATULATE','WISHES','CONGRATS','CONGRATULATIONS','SALUTE']
negative=['KILLED','MURDER','MURDERED','RAPED','KILLED','SCAM','CORRUPTION','VIOLENCE']

positive_count=0
negative_count=0
tweets_count=0

#A topicpartition ins a named tuple, consisting of topic name and partition.
tp=TopicPartition(('ktwitter'),(0))
tp1=TopicPartition(('ktwitter'),(1))

#lists the total partitions present in the topic as set , not just the ones assigned.
print("Partitions For the Given topic:\n "+str(myconsumer.partitions_for_topic('ktwitter')))

#When have to specify more than one topicpartitions enclose them in a list
myconsumer.assign([tp,tp1])
print('Assigned: ')
print(myconsumer.assignment())

myconsumer.seek_to_beginning()
#When using topic name in the consumer instance ,partitions are not assigned until iterating then seek_to_beginning will fail,

print("Offset To Read From: "+str(myconsumer.position(tp)))

for message in myconsumer:
   tweets_count=tweets_count+1
   myconsumer.poll()
   #print ("%d %d: Tweet=%s" % ( message.partition,message.offset,message.value.decode('utf-8')))
   
   for word in message.value.decode('utf-8').split():
       if word.upper() in positive:
	     positive_count=positive_count+1;
             #print("POSITIVE :"+message.value.decode('utf-8'))
			 
       elif word.upper() in negative:
	     negative_count=negative_count+1
             #print("NEGATIVE :"+message.value.decode('utf-8'))

print("Last Commited Offset for the partition: "+str(myconsumer.committed(tp)))
print("Offset for the upcoming message: "+str(myconsumer.end_offsets([tp])))			 

#Seeking to a particular offset
   
print("\nTotal Tweets processed: %d" % tweets_count)   
print(("Positive Tweets:%d \nNegative TWeets : %d")%(positive_count,negative_count))
myconsumer.close(autocommit=True)
