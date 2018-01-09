# -*- coding: utf-8 -*-
"""Main module"""
import logging
import os
import datetime
import sys
import time
import argparse
from configparser import SafeConfigParser
import DatabaseConnection
import FacebookConnect
import progressbar


def parse_config_file(path):
    global CF_PARSER
    global DATABASE_SERVER
    global DATABASE_NAME
    global DATABASE_USER
    global DATABASE_PASSWORD
    global LIMIT_POSTS
    global LIMIT_REACTIONS
    global LIMIT_COMMENTS 
    CF_PARSER = SafeConfigParser()
    if not os.path.isfile(path):
        print(path + ' was not found! Please provide a valid config file')
        sys.exit(2)
    CF_PARSER.read(path)
    DATABASE_SERVER = CF_PARSER.get('Database connection', 'servername')
    DATABASE_NAME = CF_PARSER.get('Database connection', 'databasename')
    DATABASE_USER = CF_PARSER.get('Database connection', 'user')
    DATABASE_PASSWORD = CF_PARSER.get('Database connection', 'password')
    LIMIT_POSTS = int(CF_PARSER.get('facebook_worker', 'posts_limitation'))
    LIMIT_REACTIONS = int(CF_PARSER.get('facebook_worker', 'reactions_limitation'))
    LIMIT_COMMENTS = int(CF_PARSER.get('facebook_worker', 'comments_limitation'))


def main(arguments):
    # Main program
    parse_config_file(arguments.config)
    pages = get_id_list(arguments.idlist)
    access_token = CF_PARSER.get('facebook_connection', 'accesstoken')
    since = datetime.datetime.strptime(arguments.from_datetime,"%Y%m%d%H%M").timetuple()
    until = datetime.datetime.strptime(arguments.to_datetime,"%Y%m%d%H%M").timetuple()

    for page in pages:  
        save_full_page_data(page[0], since, until,access_token,arguments.config)
        print('\n')
        print('Finished ', 'Name not defined' if len(page) < 2 else page[1],' - ',page[0],flush=True)
        time.sleep(1)

def save_full_page_data(page_id, since, until, access_token, config_path):
    """
    Extract all posts, comments and subcomment, also refering reactions
    from page between given date since and until and save in database
    """
    database_conn = DatabaseConnection.DatabaseConnection(
        DATABASE_SERVER, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
    FacebookConnect.init(access_token,config_path)
    page = FacebookConnect.get_page(page_id)
    # Retrieve pageinformation
    database_conn.insertPage(page)
    # Retrieve all posts
    posts = FacebookConnect.get_posts(page_id, LIMIT_POSTS, since, until)
    counter_posts = 0
    bar_posts = progressbar.ProgressBar(maxval=len(posts), widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar_posts.start()
    database_conn.insertPosts(posts)
    for post in posts:
        bar_posts.update(counter_posts)
        time.sleep(0.1)
        # Retrieve comments of post
        comments = FacebookConnect.get_comments(post[0], LIMIT_COMMENTS)
        database_conn.insertComments(comments)
        for comment in comments:
            # Retrieve subcomments of comment
            subcomments = FacebookConnect.get_comments(comment[0],LIMIT_COMMENTS)
            database_conn.insertComments(subcomments)
        
        counter_posts = counter_posts + 1
    bar_posts.update(len(posts))

def initialize_logger():
    """Function to initialize logger"""
    if not os.path.exists('./logs/'):
        os.makedirs('./logs/')
    logging.basicConfig(
        format='%(asctime)s;%(name)s;%(levelname)s;%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='./logs/FacebookLog.log',
        level=logging.INFO
        )

def get_id_list(path):
    return_list = []
    for f in open(path).readlines():
        if f[0] != '#':
            return_list.append(tuple(f.rstrip().split(';')))
    return return_list

if __name__ == "__main__":
    initialize_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('config',help='Location of config file')
    parser.add_argument('idlist',help='Location of Facebook ID list to extract. File: CSV without header - Format: ID;Name of Page')
    parser.add_argument('from_datetime',help='Start datetime to extract. Format: YYYYMMDDHHmm')
    parser.add_argument('to_datetime',help='End datetime to extract. Format: YYYYMMDDHHmm')
    args = parser.parse_args()
    logging.info('Start session')
    main(args)
