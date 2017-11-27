# ttwitter-Kafka-Producer-and-consumer

This Project uses python's tweepy module to Read Tweets from specific handles twitter using various API's.
These tweets are written into a predefined topic using kafka-python producer.
kafka consumer reads from these topics and does some basic analysis on if the tweet is a positive or negative one.
