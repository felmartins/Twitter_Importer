### Please note that while the cluster passed the 60.000k / 3 day test this code is not yet final

import warnings
import os
from twarc import Twarc
import pandas as pd
import numpy as np
import csv
import re
import json
import logging
import time
from datetime import datetime, timedelta
import gzip
from urllib import request
import sqlite3
from sqlite3 import Error
from credentials import consumer_key, consumer_secret, access_token, access_token_secret

#os.chdir('/home/pi/clusterfs/Twitter')

warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None  # default='warn'
formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)
timenow = time.strftime("%d%m%Y-%H%M%S")
logfile = f"{timenow}.log"

### Method for creating and entering log entries
def LOG_insert(file, format, text, level):
    infoLog = logging.FileHandler(file)
    infoLog.setFormatter(format)
    logger = logging.getLogger(file)
    logger.setLevel(level)
    if not logger.handlers:
        logger.addHandler(infoLog)
        if (level == logging.INFO):
            logger.info(text)
        if (level == logging.ERROR):
            logger.error(text)
        if (level == logging.WARNING):
            logger.warning(text)
    infoLog.close()
    logger.removeHandler(infoLog)
    return

### Creates SQL database for processed data
def create_connection(db_file = "database.db", collect_file = "collect_data.db", table_sql = "sqlcode.sql", collect_sql = "collectcode.sql"):
    conn = None
    try:
        LOG_insert(logfile, formatLOG, f"Creating databases...", logging.INFO)
        conn = sqlite3.connect(db_file)
        LOG_insert(logfile, formatLOG, f"Creating empty tables inside database...", logging.INFO)
        c = conn.cursor()
        with open(table_sql, 'r') as sql_file:
            conn.executescript(sql_file.read())
        conn.close()
        conn = sqlite3.connect(collect_file)
        LOG_insert(logfile, formatLOG, f"Creating empty tables inside database...", logging.INFO)
        c = conn.cursor()
        with open(collect_sql, 'r') as sql_file:
            conn.executescript(sql_file.read())
    except Error as e:
        LOG_insert(logfile, formatLOG, f"Error creating database: {e}", logging.ERROR)
        quit()
    finally:
        if conn:
            conn.close()

### Develops a method for sending collected data to SQL database, splitting into tables.
def send_data(data, db_file = "database.db"):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute(r"SELECT name FROM sqlite_master WHERE type = 'table';")
        testcond = cursor.fetchone()
        names = []
        while testcond != None:
            names.append(testcond[0])
            testcond = cursor.fetchone()
        for i in range(len(names)):
            cursor.execute("SELECT * from "+names[i]+" ORDER BY ROWID ASC LIMIT 1")
            colnames = list(map(lambda x: x[0], cursor.description))
            colnames = [description[0] for description in cursor.description]
            segDB = data[colnames]
            LOG_insert(logfile, formatLOG, f"Sending data to table {names[i]} on database...", logging.INFO)
            segDB.to_sql(names[i], con = conn, if_exists = "append", index = False)
    except Error as e:
        LOG_insert(logfile, formatLOG, f"Error sending data to database: {e}", logging.ERROR)
        quit()
    finally:
        with open("sanity.sql", 'r') as sql_file:
            conn.executescript(sql_file.read())    
        if conn:
            conn.close()

### Downloads and unpacks zipped files from GitHub repo
def download_unpack(testdate):
    if isinstance(testdate, datetime):
        cordate = datetime.strftime(testdate, "%Y-%m-%d")
    else:
        LOG_insert(logfile, formatLOG, f"Something wrong with date handling... Revise code", logging.ERROR)
        return
    dailies_url= "https://raw.githubusercontent.com//thepanacealab/covid19_twitter/master/dailies/"
    path = f"{dailies_url}{cordate}/{cordate}-dataset.tsv.gz"
    try:
        with request.urlopen(path) as response:
            with gzip.GzipFile(fileobj=response) as uncompressed:
                file_content = uncompressed.read()
        with open("data.tsv", 'wb') as f:
             f.write(file_content)
             return
    except Exception as e:
        LOG_insert(logfile, formatLOG, f"Database Error: {e}", logging.ERROR)
        quit()


### Collects twit ID information from GitHub data file
def tweet_id_collection(test = False):
    ids = []
    with open("data.tsv",encoding='utf8') as tsvfile:
        tsvreader = csv.reader(tsvfile, delimiter="\t")
        next(tsvreader, None)
        LOG_insert(logfile, formatLOG, f"Collecting Twit ID`s from file", logging.INFO)
        for row in tsvreader:
            ids.append(row[0])
    if test == True:
        ids = ids[0:60000]
    return ids

## Cleansup and selects data from the resulting JSON file.
def cleanup(dirty_db):
    try:
        cleandb = dirty_db[["id","user.id", "created_at", "lang", "full_text", "retweeted_status.id", "in_reply_to_status_id", "in_reply_to_user_id", "entities.hashtags", "entities.urls", "entities.user_mentions", "quoted_status_id", "user.screen_name", "user.name", 
                            "user.location", "user.created_at", "user.description", "user.followers_count", "user.friends_count", "user.listed_count", "user.favourites_count", "user.statuses_count", "user.verified", "user.default_profile_image", "retweet_count", 
                            "favorite_count", "retweeted_status.user.id", "quoted_status.user.id"]]
        cleandb = cleandb[cleandb["lang"].isin(["de", "en", "pt"])==True]
        cleandb.loc[:,'created_at'] = pd.to_datetime(cleandb.loc[:,'created_at'], format = "%a %b %d %H:%M:%S %z %Y")
        cleandb.loc[:,'created_at'] = cleandb.loc[:,'created_at'].dt.tz_localize(None)
        cleandb.loc[:,'created_at'] = (cleandb.loc[:,'created_at'] - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's')
        tempurl = cleandb['entities.urls'].astype(str)
        for i in tempurl.index:
            tempurl[i] = re.findall(r"(?!https://t.co/*)https{0,1}:\/\/\S*", tempurl[i])
            cleandb['entities.urls'] = tempurl.astype(str)
        tempmentions = cleandb['entities.user_mentions'].astype(str)
        for i in tempmentions.index:
            tempmentions[i] = re.findall(r"\d+',", tempmentions[i])
            cleandb['entities.user_mentions'] = tempmentions.astype(str)
        temphash = cleandb['entities.hashtags'].astype(str)
        for i in temphash.index:
            temphash[i] = re.sub(r'[^A-Za-z0-9 ]+', "", temphash[i])
            temphash[i] = re.findall(r"text (?:\w+)", temphash[i])
            temphash[i] =[s.replace("text ", "") for s in temphash[i]]
            cleandb['entities.hashtags'] = temphash.astype(str)
        cleandb.columns = cleandb.columns.str.replace(".", "_")
    except Exception as e:
        LOG_insert(logfile, formatLOG , f"Error cleaning data: {e}", logging.ERROR)
        quit()

    return cleandb

### Processed collected information from Tweet ID`s in batches, then sends the data to the SQL database.
def process_tweets(ids, batchsize = 1000, batch = 1, logday = None):
    tt = time.process_time()
    start = (batchsize * batch) - batchsize
    end = (batchsize * batch) - 1
    pre_count = len(ids)
    try:
        while start < pre_count:
            hyd_row_count = 0
            post_row_count = 0
            tb = time.process_time()  
            LOG_insert(logfile, formatLOG, f"Starting file collection on Batch {batch}", logging.INFO)
            segdata = ids[start:end]
            LOG_insert(logfile, formatLOG, f"Dumping test tweets from {start+1} to {end+1}", logging.INFO)
            tweets = []
            with open("dump.json", 'w', encoding='utf-8') as outfile:
                for tweet in t.hydrate(segdata):
                        tweets.append(tweet)
                        json.dump(tweet, outfile)
                        outfile.write('\n') 
            dirty_db = pd.json_normalize([json.loads(x) for x in open("dump.json").readlines()])
            hyd_row_count += len(dirty_db.index)
            procdate = logday.date()
            cleandb = cleanup(dirty_db)
            post_row_count += len(cleandb.index)
            LOG_insert(logfile, formatLOG , f"Sending batch {batch} to SQL database", logging.INFO)
            send_data(cleandb)
            open('dump.json', 'w').close()
            start+= batchsize
            end+= batchsize
            elapsed_batchtime = time.process_time() - tb
            LOG_insert(logfile, formatLOG , f"Completed batch operations in {elapsed_batchtime}", logging.INFO)
            tw_collect = pd.DataFrame({"day": [procdate], "batch":[batch], "dataset_tweets": [len(segdata)+1], "collected_tweets": [hyd_row_count], "cleaned_tweets": [post_row_count]})
            conn = sqlite3.connect("collect_data.db")
            LOG_insert(logfile, formatLOG, f"Sending day {procdate}, batch {batch} numbers to collection database...", logging.INFO)
            tw_collect.to_sql("tw_collect", con = conn, if_exists = "append", index = False)
            conn.close()
            batch+=1
    except Exception as e:
        LOG_insert(logfile, formatLOG , f"Error fectching batch {batch}: {e}", logging.ERROR)
        quit()
    finally:
        elapsed_totaltime = time.process_time() - tt
        LOG_insert(logfile, formatLOG , f"Completed day file operations in {elapsed_totaltime}", logging.INFO)

### Main activation function (will sort into a init later)
def mainjob(test = False, dayresume = False, resumefromdate = "22-03-2020", resumefrombatch = 0):
    run = 1
    LOG_insert(logfile, formatLOG , "Starting new log...", logging.INFO)
    create_connection()
    cor_resumedate = datetime.strptime(resumefromdate, "%d-%m-%Y")
    iter_date = cor_resumedate
    if iter_date <= datetime.today():
        while iter_date <= datetime.today():
            LOG_insert(logfile, formatLOG , f"Starting operations on {iter_date}", logging.INFO)
            download_unpack(testdate = iter_date)
            if test == True:
                ids = tweet_id_collection(test = True)
            else:
                ids = tweet_id_collection()
            if resumefrombatch != 0:
                process_tweets(ids, batch = resumefrombatch, logday = iter_date)
            else:
                process_tweets(ids, logday= iter_date)
            if dayresume == True:
                LOG_insert(logfile, formatLOG , "Fault day tweets recovered... rexecute the script with resumefromdate being set to the next valid date", logging.INFO)
                break
            elif test == True:
                if run >= 3:
                    LOG_insert(logfile, formatLOG , "3 Day-60.000k tweets per day test run complete...", logging.INFO)
                    break
                else:
                    iter_date = iter_date + timedelta(1)
                    run +=1
            else: 
                iter_date = iter_date + timedelta(1)
                run +=1
    else:
        LOG_insert(logfile, formatLOG , "Operations Concluded...", logging.INFO)

mainjob(test=True)