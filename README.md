# ttwitter-Kafka-Producer-and-consumer

This Project uses Kafka-python to Read Tweets from specific handles twitter using API's provided by tweepy module.
These tweets are written into a predefined topic using kafka producer.
kafka consumer reads from these topics and does some basic analysis on if the tweet is a positive or negative one.
