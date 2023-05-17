import tweepy
from pathlib import Path
from Scrape_functions import *
from Management_functions import *

# Set path to the database
api_keys_path = Path("D:/Research/")
directory_path = Path("D:/Research/Twitter Data")
db_path = directory_path/"Brand_Twitter.db"

# API Credentials File Names
filenames = ["API.txt","APIsecret.txt","bToken.txt"]

# Get API Credentials
Creds = {}
for file in filenames:
    key = file.split('.')[0]
    with open(api_keys_path/file, "r") as f:
        value = f.read()
    Creds[key] = value

# Create the client to query the API with
client = tweepy.Client(bearer_token = Creds["bToken"],consumer_key = Creds["API"],
consumer_secret = Creds["APIsecret"], wait_on_rate_limit = True)

# Get Input Data Ready
start_date, end_date = CollectTimePoints(filename = directory_path/"start_time.txt")
brand_ids = CollectInputIDs(filename = directory_path/"id_list.txt")
print("Total ids Acquired: " + str(len(brand_ids)))

# Scrape User Data
print("Starting to Scrape Data")
dfUsers, dfErrorUsers = CollectUserData(user_ids = brand_ids, date = end_date)

# Account for any problems
print("Checking for Errors")
FlagProblemIDs(ProblemIDs = dfErrorUsers, filename = directory_path/"ProblemIDs.csv", end_date = end_date)

# Get New Tweets
print("Starting to get data for tweets")
TweetMetricsNew, dfTweetAtts = GetNewTweets(user_ids = brand_ids, start_date = start_date, end_date = end_date)

# Get the IDs for past tweets
print("Updating metrics for existing tweets")
past_tweet_ids = CollectPastTweets(db_path = db_path)
past_tweets_grouped = SplitPastTweets(string_list = past_tweet_ids)
print("Total IDs to update: ", len(past_tweet_ids))
print("Queries to push: " + str(len(past_tweets_grouped)))

# Get Info on past tweets
TweetMetricsPast = CollectPastTweetsMetrics(past_tweets_grouped)
print("Metrics updated")

# Merge with Current tweets metrics
print("Preparing data for push to database")
dfTweetMetrics = MergeTweetMetrics(TweetMetricsNew, TweetMetricsPast)

# Correct Format of Columns
dfTweetAttachments, dfTweetAttContent = CorrectFormat(df = dfTweetAtts)

# Push these to the dataframe
print("Adding new data to database")
AddScrapedData(dfUsers,dfTweetMetrics,dfTweetAttachments,dfTweetAttContent,db_path)

#Change the time
print("Data added, setting new start time for next run")
SetNewStartTime(filename = directory_path/"start_time.txt",end_time = end_date)
