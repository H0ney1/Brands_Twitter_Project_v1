import pandas as pd
import tweepy
import time
import datetime

# API Credentials File Names
############# THESE ARE STORED AS SEPARATE .txt FILES IN THE SAME FOLDER
filenames = ["API.txt","APIsecret.txt","bToken.txt"]

# Get API Credentials
Creds = {}
for file in filenames:
    key = file.split('.')[0]
    with open(file, "r") as f:
        value = f.read()
    Creds[key] = value

# Create the client to query the API with
client = tweepy.Client(bearer_token = Creds["bToken"],wait_on_rate_limit = True)

# Function to get the user IDs
def CollectInputIDs(filename):
    with open(filename, 'r') as file:
        lines = file.read().splitlines()

    return lines

# Function to get data
def CollectUserData(user_ids, date):
    # Create empty list for a dataframe
    d = []    
    # Create empty list for problematic IDs
    problem = []
    # Iterate over a list of user ids
    for id in user_ids:
        # Get desired user fields
        try:
            user = client.get_user(id = id, user_fields = ["created_at","description","location","name","public_metrics","username","verified","verified_type","url"])
            print("Working on " + id)
        # Capture faulty ids
        except tweepy.errors.TweepyException as e:
            print("error with " + id)
            probleminfo = {
                "Brand_ID":id,
                "Error":str(e)
            }
            problem.append(probleminfo)
            continue
        # Set up a variable for user data
        ud = user.data
        # Append to the list of dictionaries
        userinformation = {
                "Date":date,
                "Brand_ID":id,
                "Username":ud.username,
                "Date_Created":ud.created_at,
                "Location":ud.location,
                "Display_Name":ud.name,
                "Description":ud.description,
                "Followers":ud.public_metrics["followers_count"],
                "Following":ud.public_metrics["following_count"],
                "Tweet_Count":ud.public_metrics["tweet_count"],
                "Listed_Count":ud.public_metrics["listed_count"],
                "Verified":(1 if ud.verified else 0),
                "Verified_Type":ud.verified_type,
                "URL":ud.url                
            }
        d.append(userinformation)
        time.sleep(1)
    #Create a DataFrame from the list
    dfUsers = pd.DataFrame(d)
    dfErrors = pd.DataFrame(problem)
    return dfUsers, dfErrors

# Function for getting the metrics
def CollectTweetMetrics(tweet,end_date):
    TweetMetricsData = {
                    "Date":end_date,
                    "Date_Created":tweet.created_at,
                    "Brand_ID":tweet.author_id,
                    "Tweet_ID":tweet.id,
                    "Likes":tweet.public_metrics.get("like_count"),
                    "Comments":tweet.public_metrics.get("reply_count"),
                    "Retweets":tweet.public_metrics.get("retweet_count"),
                    "Quotes":tweet.public_metrics.get("quote_count"),
                    "Views":tweet.public_metrics.get("impression_count")
                }
    return(TweetMetricsData)

# Collect Attachments Function
def CollectTweetAttachments(tweet,media):
    # Set up attachments capture
    attachments = tweet.data.get('attachments',{})
    media_keys = attachments.get('media_keys',[])
    # Set up Dictionary for results
    Results_to_add = {}
    # Capture Hashtags, Mentions and URLs
    attachmentType = ["mentions","hashtags","urls"]
    attachmentKey = ["id","tag","url"]
    # Get Attachments Dictionary
    for attType, attKey in zip(attachmentType, attachmentKey):
        try:
            attData = [t[attKey] for t in tweet.entities.get(attType,[])]
        except AttributeError:
            attData = []
        Results_to_add[attType] = attData
    # Get Photo/Video Attachments
    Results_to_add["AttachmentsCount"] = len(media_keys)
    Results_to_add["AttachmentsType"] = [media.get(t).type for t in media_keys]
    Results_to_add["ImageUrls"] = [media.get(t).get('url') for t in media_keys]
    Results_to_add["VideoUrls"] = [media.get(t).get("variants",[{}])[0].get("url") for t in media_keys]
    # Return Dictionary of attachments
    return(Results_to_add)

# Function for getting additional tweet data
def CollectTweetData(tweet,TweetAttachments,end_date):
    tweet_information = {
                    "Date":end_date,
                    "Date_Created":tweet.created_at,
                    "Brand_ID":tweet.author_id,
                    "Tweet_ID":tweet.id,
                    "Reply_To":tweet.in_reply_to_user_id,
                    "Text":tweet.text,
                    "Mentions_Count":len(TweetAttachments["mentions"]),
                    "Mentions_ID":TweetAttachments["mentions"],
                    "Hashtags_Count":len(TweetAttachments["hashtags"]),
                    "Hashtags":TweetAttachments["hashtags"],
                    "URLs_Count":len(TweetAttachments["urls"]),
                    "URLs":TweetAttachments["urls"],
                    "Attachments_Count":TweetAttachments["AttachmentsCount"],
                    "Attachments_Type":TweetAttachments["AttachmentsType"],
                    "Image_URLs":TweetAttachments["ImageUrls"],
                    "Video_URLs":TweetAttachments["VideoUrls"],
                    "Conversation_ID":tweet.conversation_id
                }
    return(tweet_information)

# Function to store the errors
def FlagProblemIDs(ProblemIDs,filename,end_date):
    # Get the problematic IDs
    try:
        df = pd.read_csv(filename)
    except:
        df = pd.DataFrame()
    # Add the date
    ProblemIDs["Date"] = end_date
    print(ProblemIDs.head())
    # Add the problematic IDs
    df = pd.concat([df, ProblemIDs], ignore_index = True)
    # Save updated Dataframe to CSV
    df.to_csv(filename)


# Function to get new tweets posted by brand accounts
def GetNewTweets (user_ids,start_date,end_date):
    # Aspects of the tweet to be fetched
    tweet_info = [
    "attachments","author_id","conversation_id","created_at","edit_controls","entities","geo","id", 
    "in_reply_to_user_id","lang","public_metrics","possibly_sensitive","referenced_tweets","reply_settings", 
    "source","text"
    ]
    # What media fields to get
    media_info = ["type","url","public_metrics","variants"]
    # Create empty lists which will be turned into the dataframes
    metrics = []
    twinfo = []
    
    for id in user_ids:
        print("Fetching recent tweets for " + id)
        # Set the query per account
        q = "from:" + id + " -is:retweet"
        tweets = client.search_all_tweets(query = q, start_time = start_date, end_time = end_date, tweet_fields = tweet_info,
                                          expansions = "attachments.media_keys",
                                          media_fields = media_info, max_results = 100)
        # Get Attachments
        media = {m["media_key"]:m for m in tweets.includes.get('media',[])}
        
        # Iterate over each tweet for each account
        if tweets.data is None:
            print(id + "Has no tweets today")
            time.sleep(2)
            continue
        else:
            for tweet in tweets.data:
                # Get metrics data
                metrics_data = CollectTweetMetrics(tweet,end_date = end_date)
                metrics.append(metrics_data)            
                
                # Get attachments for the tweet
                TweetAttachments = CollectTweetAttachments(tweet,media)
                # Gather Tweet data information
                tweet_data = CollectTweetData(tweet,TweetAttachments,end_date)
                twinfo.append(tweet_data)
        # Slight Time Delay per account
        time.sleep(1)
        
    # Create Dataframes:
    dfMetrics = pd.DataFrame(metrics)
    dfTWinfo = pd.DataFrame(twinfo)
    
    # Get output as tuple
    return dfMetrics, dfTWinfo

# Get Metrics for past tweets
def CollectPastTweetsMetrics(past_tweet_grouped_ids):
    if not past_tweet_grouped_ids:
        return pd.DataFrame()
    else:
        # Empty list for dataframe
        metrics_past = []
        # Define what the query will be:
        tweet_info = ["author_id","created_at","id","public_metrics"]
        # Iterate over each tweet ID
        for ids in past_tweet_grouped_ids: 
            tweets = client.get_tweets(ids=ids, tweet_fields = tweet_info)
            # Get Datetime
            cur_date = datetime.datetime.now()
            cur_date = cur_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            # Iterate over each tweet
            for tweet in tweets.data:
                cur_metrics = CollectTweetMetrics(tweet,end_date = cur_date)
                metrics_past.append(cur_metrics)
            # Short Rest
            time.sleep(1)
        # Get output as df
        dfPastMetrics = pd.DataFrame(metrics_past)

        return dfPastMetrics
