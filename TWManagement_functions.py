import datetime
import sqlite3
import os
import pandas as pd

# Get the Start and End Datetimes function
def CollectTimePoints(filename):
    # Get the file open with current start time
    with open(filename,'r') as file:
        start_time = file.read()
    # Set the current time as end_time
    end_time = datetime.datetime.now()
    # Format End Time
    end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Return the variables
    return start_time, end_time

# Save New Start time
def SetNewStartTime(filename,end_time):
    with open(filename,'w') as file:
        file.write(str(end_time))

# Get Tweet IDs from the table 
def CollectPastTweets(db_path):
    # Check if database exists
    if not os.path.exists(db_path):
        return []
    # Get the unique tweets
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Query the tweets
    c.execute("SELECT DISTINCT Tweet_ID FROM Tweets_Metrics")
    tweet_ids = [row[0] for row in c.fetchall()]
    # Close connection
    c.close()
    conn.close()

    return tweet_ids

# Put Past Tweet IDs into workable form - list of lists
def SplitPastTweets(string_list, max_size = 100):
    return [string_list[i:i+max_size] for i in range(0,len(string_list),max_size)]

# Merge Old tweet metrics with new for the time period
def MergeTweetMetrics(TweetMetricsNew,TweetMetricsPast):
    result = pd.concat([TweetMetricsNew, TweetMetricsPast])
    return result

def CorrectFormat(df):
    # Define a list of column names to be fetched
    columns_to_fetch = ["Tweet_ID","Mentions_ID","Hashtags","URLs","Attachments_Type","Image_URLs","Video_URLs"]
    columns_to_keep = ["Date","Date_Created","Brand_ID","Tweet_ID","Reply_To",
    "Text","Mentions_Count","Hashtags_Count","URLs_Count","Attachments_Count","Conversation_ID"]
    # Two dataframes - one for content, one for count
    dfAttContent = df[columns_to_fetch]
    dfAttCount = df[columns_to_keep]
    # Get Attachment Content into correct format
    for c in columns_to_fetch[1:]:
        dfAttContent = dfAttContent.explode(c)
    
    dfAttContent = pd.melt(dfAttContent,id_vars = "Tweet_ID",value_vars = columns_to_fetch[1:])
    dfAttContent = dfAttContent.rename(columns={'variable':'Additional_Type','value':'Additional_Values'})

    return dfAttCount,dfAttContent
    

# Push the dataframes as tables to the database:
def AddScrapedData(dfUsers, dfTweetMetrics, dfTweetAtts,dfTweetAttsContent, db_path):
    # Set Connection
    conn = sqlite3.connect(db_path)
    # Append each df
    dfUsers.to_sql('Brand_Accounts', conn, if_exists = "append")
    dfTweetMetrics.to_sql('Tweets_Metrics', conn, if_exists = "append")
    dfTweetAtts.to_sql('Tweets_Attributes', conn, if_exists = "append")
    dfTweetAttsContent.to_sql('Tweets_Attributes_Detailed', conn, if_exists = "append")
    # Close connection
    conn.commit()
    conn.close()
