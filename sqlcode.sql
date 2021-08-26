 --Creates empty tables

CREATE TABLE IF NOT EXISTS tw_meta (
    id INTEGER UNIQUE,
    user_id INTEGER,
    created_at INTEGER,
    lang TEXT
);

CREATE TABLE IF NOT EXISTS tw_cnt (
    id INTEGER UNIQUE,
    full_text TEXT,
    in_reply_to_status_id INTEGER,
    entities_hashtags TEXT,
    entities_urls TEXT,
    quoted_status_id INTEGER,
    retweeted_status_id INTEGER,
    retweet_count INTEGER,
    favorite_count INTEGER,

    FOREIGN KEY (id)s
        REFERENCES tw_meta (id)
); 

CREATE TABLE IF NOT EXISTS tw_user (
    user_id INTEGER,
    user_name TEXT,
    user_screen_name TEXT,
    user_location TEXT,
    user_description TEXT,
    user_statuses_count INTEGER,
    user_verified INTEGER,
    user_followers_count INTEGER,
    user_friends_count INTEGER,
    user_listed_count INTEGER,
    user_favourites_count INTEGER
); 

CREATE TABLE IF NOT EXISTS tw_network (
    id INTEGER UNIQUE,
    user_id INTEGER,
    in_reply_to_user_id INTEGER,
    retweeted_status_user_id INTEGER,
    quoted_status_user_id INTEGER,
    entities_user_mentions TEXT,

    FOREIGN KEY (id)
        REFERENCES tw_meta (id),
    FOREIGN KEY (user_id)
        REFERENCES tw_user (user_id)
);