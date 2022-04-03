import requests
import json
import time
import math
import pandas as pd
import os
join = os.path.join

CURR_PATH = os.path.dirname(__file__)
TWEETS_DIR = "../../../data/tweets"


# Load api keys
with open(join(CURR_PATH, "api_keys.json"), "r") as f:
    keys_dict = json.load(f)
BEARER_TOKEN = keys_dict["academic"]["bearer_token"]
#BEARER_TOKEN = keys_dict["essential"]["bearer_token"]


def get_empty_tweets_dict():
    res_dict = {"tweet_id": [], "text": [], "author_id": [], "conversation_id": [],
           "reply_settings": [], "created_at": [], "in_reply_to_user_id": [],
           "lang": [], "retweet_count": [], "reply_count": [], "like_count": [],
           "quote_count": [], "possibly_sensitive": [], "reply_settings": [],
           "reference_type": [], "referenced_tweet_id": [], "hashtags": []}
    return res_dict


def save_tweets_to_file(res_dict, name, idx):
    df = pd.DataFrame(res_dict)
    df.to_csv(join(TWEETS_DIR, "{}_{}.csv".format(name, idx)), index=False)


def add_tweets(res_dict, tweet_obj_list):
    """Place all attributes of interest in res_dict"""
    for obj in tweet_obj_list:
        # Attributes that exist in all responses
        res_dict["text"] += [obj["text"]]
        res_dict["tweet_id"] += [obj["id"]]
        res_dict["author_id"] += [obj["author_id"]]
        res_dict["conversation_id"] += [obj["conversation_id"]]
        res_dict["reply_settings"] += [obj["reply_settings"]]
        res_dict["created_at"] += [obj["created_at"]]
        res_dict["possibly_sensitive"] += [obj["possibly_sensitive"]]
        for metric_name, count in obj["public_metrics"].items():
            res_dict[metric_name] += [count]
        
        # Attributes that may not exist in all responses
        if "entities" in obj.keys() and "hashtags" in obj["entities"].keys():
            hashtag_list = []
            for x in obj["entities"]["hashtags"]:
                hashtag_list += [x["tag"]]
            res_dict["hashtags"] += [' '.join(hashtag_list)]
        else:
            res_dict["hashtags"] += [None]
        if "in_reply_to_user_id" in obj.keys():
            res_dict["in_reply_to_user_id"] += [obj["in_reply_to_user_id"]]
        else:
            res_dict["in_reply_to_user_id"] += [None]
        if "lang" in obj.keys():
            res_dict["lang"] += [obj["lang"]]
        else:
            res_dict["lang"] += [None]
        if "referenced_tweets" in obj.keys():
            res_dict["referenced_tweet_id"] += [obj["referenced_tweets"][0]["id"]]
            res_dict["reference_type"] += [obj["referenced_tweets"][0]["type"]]
        else:
            res_dict["referenced_tweet_id"] += [None]
            res_dict["reference_type"] += [None]


def get_base_url(start_time, next_token=None, max_results=100):
    base_url = "https://api.twitter.com/2/tweets/search/all?"
    base_url += "start_time={}&".format(start_time)  # only consider tweets after this datetime
    base_url += "max_results={}".format(max_results)  # maximum number of results to be returned

    if next_token is not None:  # retrieve next batch of results
        base_url += "&next_token={}".format(next_token)

    return base_url


def get_fields():
    """Reference: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet"""
    field_list = [
        "id",  # unique identifier of the tweet
        "text",  # text of the tweet
        "author_id",  # unique identifier of the author of the tweet
        "conversation_id",  # identifier of the "reply tree" of the tweet
        "reply_settings",  # shows who can reply to a given tweet
        "created_at",  # datetime of the tweet
        "entities",  # includes tweet hashtags
        "in_reply_to_user_id",  # if tweet is a reply, the id of tweet it is replying to
        "lang",  # language
        "possibly_sensitive",  # whether or not the tweet may contain sensitive content
        "public_metrics",  # engagement statistics
        "referenced_tweets"  # a list of tweets that the tweet refers to
    ]
    field_str = ','.join(field_list)
    return field_str


def get_query(keyword_str=None, conv_id=None, root_only=True):
    """Reference: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query"""
    query_str = ""
    if keyword_str is not None:
        query_str += '"{}"'.format(keyword_str)
    if conv_id is not None:
        query_str += ' conversation_id:{}'.format(conv_id)
    if root_only:  # root tweets only (no RTs, replies, quotes)
        query_str += ' -is:retweet -is:reply -is:quote'

    return query_str


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r


def connect_to_endpoint(search_url, params):
    while True:
        response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    
        time.sleep(1)  # rate limit of 1 sec / 1 request
        if response.status_code == 429:  # too many requests
            print("Too many requests. Waiting for 15 min...")
            time.sleep(901)  # wait for 15 min
        else:
            break

    if response.status_code != 200:
        print("Request returned an error: {} {}".format(
                response.status_code, response.text))
        return response.json(), False
    return response.json(), True


def api_search(start_time, next_token, keyword_str=None, conv_id=None):
    """
    Perform a single call to Twitter API.
    Required: start_time, next_token.
    Optional: keyword_str, conv_id
    """
    base_url = get_base_url(start_time, next_token=next_token)
    field_str = get_fields()
    query_str = get_query(keyword_str, conv_id)
    params = {"query": query_str, "tweet.fields": field_str}
    json_response, success = connect_to_endpoint(base_url, params)
    #print(json_response)
    try:
        return json_response["data"], json_response["meta"], success, base_url, params
    except KeyError:
        return None, None, success, base_url, params


def retrieve_initial_keywords(filename, save_filename):
    # Read file with keywords
    df_keywords = pd.read_csv(filename)
    keywords = df_keywords["keyword"].to_list()

    res_dict = get_empty_tweets_dict()
    calls_per_keyword = 1
    start_time = "2021-03-13T00:00:00Z"

    for i, keyword in enumerate(keywords):
        print("Retrieving keyword {}/{}: {}".format(i + 1, len(keywords), keyword))
        res_dict = get_empty_tweets_dict()
        
        next_token = None
        j = 0
        while j < calls_per_keyword:
            obj_list, meta, success, url, params = api_search(
                start_time, next_token, keyword_str=keyword)
            
            if not success:
                print(url, "\n", params)
            elif obj_list is not None:
                add_tweets(res_dict, obj_list)

            if meta is None or "next_token" not in meta.keys():
                break
            next_token = meta["next_token"]
            j += 1

        # Save for every keyword
        save_tweets_to_file(res_dict, save_filename, keyword)


if __name__ == '__main__':
    retrieve_initial_keywords("anxiety_keywords_2 - Sheet1.csv", "2_initial_keyword_tweets")