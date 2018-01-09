# -*- coding: utf-8 -*-
"""Module to extract Facebookdata, using GraphAPI"""
import facebook
import requests
import json
import urllib.parse
import urllib.request
import time
import datetime
import sys
import logging
import os
import random
from fake_useragent import UserAgent
from configparser import SafeConfigParser

def parse_config_file(config_path):
    global CF_PARSER
    global RETRY_CONNECTION_WAIT
    global USER_AGENT

    CF_PARSER = SafeConfigParser()
    if not os.path.isfile(config_path):
        print(config_path + ' was not found! Please provide file in config/app.config')
        sys.exit(2)
    CF_PARSER.read(config_path)
    RETRY_CONNECTION_WAIT = int(CF_PARSER.get('facebook_worker', 'retry_wait_seconds'))
    USER_AGENT = UserAgent()


__reactions = ['LIKE','LOVE','WOW','HAHA','ANGRY','SAD']
__reactions_query = ','.join(['reactions.type({}).summary(total_count).limit(0).as(react_{})'.format(r, r.lower()) for r in __reactions])

__page_fields = ['id','name','website','fan_count']
__post_fields = ['id','created_time','updated_time','type','shares','message','link','from','attachments',__reactions_query]
__comment_fields = ['id','created_time','updated_time','message','like_count','from','attachment',__reactions_query]
__reaction_fields = ['type','id','name'] #  ID ist ID of user
__tagged_fields = ['id','created_time','updated_time','from','message','status_type','target']


def init(access_token, config_path):
    global __graph
    try:
        __graph = facebook.GraphAPI(access_token=access_token, version='2.7')
    except facebook.GraphAPIError as e:
        logging.info('[graph] Error retrieving graph - Message: %s', e)
    parse_config_file(config_path)

def convert_dict_tuple(dict_var, fields):
    """
    Convert given dictionary to tuple, where only value fields are used
    Filter dictionary with given fields list
    """
    data_tuple = ()
    for field in fields:
        tuple_entry = dict_var.get(field,0)
        if(field == 'created_time' or field == 'updated_time'):
            try:
                tuple_entry = datetime.datetime.strptime(tuple_entry, '%Y-%m-%dT%H:%M:%S%z')
            except TypeError:
                tuple_entry = None
        #Iterate trough dict and add values to tuple    
        if(type(tuple_entry) is dict):
            tuple_entry = dict_value
            data_tuple = data_tuple + (tuple_entry,)
        else:
            data_tuple = data_tuple + (tuple_entry,)
    return data_tuple

def prepare_post_tuple(post):
    """
    Prepare post tuple in following format:
    (id_of_post,created_time,updated_time,type_of_post,number_of_shares,message,link,EMPTY,EMPTY,EMPTY,
    number_of_like,number_of_love,number_of_wow,number_of_haha,number_of_angry,number_of_sad,name_of_user,id_of_user)
    Todo: EMPTY,EMPTY,EMPTY is attachment_type,attachment_title and attachment_url. Need to be build in data schema
    """
    return_tuple = ()
    return_tuple = return_tuple + (post.get('id'),)
    return_tuple = return_tuple + (datetime.datetime.strptime(post.get('created_time'), '%Y-%m-%dT%H:%M:%S%z'),)
    if post.get('updated_time') is None:
        return_tuple = return_tuple + (None,)
    else:
        return_tuple = return_tuple + (datetime.datetime.strptime(post.get('updated_time'), '%Y-%m-%dT%H:%M:%S%z'),)
    if post.get('type') is None:
        return_tuple = return_tuple + ('',)
    else:
        return_tuple = return_tuple + (post.get('type'),)
    if post.get('shares') is None:
        return_tuple = return_tuple + (0,)
    else:
        return_tuple = return_tuple + (post.get('shares').get('count',0),)
    if post.get('message') is None:
        return_tuple = return_tuple + ('',)
    else:
        return_tuple = return_tuple + (post.get('message',''),)
    if post.get('link') is None:
        return_tuple = return_tuple + ('',)
    else:
        return_tuple = return_tuple + (post.get('link',''),)
    return_tuple = return_tuple + ('',)#(post.get('attachments'),) type
    return_tuple = return_tuple + ('',)#(post.get('attachments'),) title
    return_tuple = return_tuple + ('',)#(post.get('attachments'),)url
    if post.get('react_like') is None:
        return_tuple = return_tuple + (0,)
    else:
        return_tuple = return_tuple + (post.get('react_like').get('summary').get('total_count'),)
    if post.get('react_love') is None:
        return_tuple = return_tuple + (0,)
    else:
        return_tuple = return_tuple + (post.get('react_love').get('summary').get('total_count'),)
    if post.get('react_wow') is None:
        return_tuple = return_tuple + (0,)
    else:
        return_tuple = return_tuple + (post.get('react_wow').get('summary').get('total_count'),)
    if post.get('react_haha') is None:
        return_tuple = return_tuple + (0,)
    else:
        return_tuple = return_tuple + (post.get('react_haha').get('summary').get('total_count'),)
    if post.get('react_angry') is None:
        return_tuple = return_tuple + (0,)
    else:
        return_tuple = return_tuple + (post.get('react_angry').get('summary').get('total_count'),)
    if post.get('react_sad') is None:
        return_tuple = return_tuple + (0,)
    else:
        return_tuple = return_tuple + (post.get('react_sad').get('summary').get('total_count'),)
    return_tuple = return_tuple + (post.get('from').get('name'),)
    return_tuple = return_tuple + (post.get('from').get('id'),)
    return return_tuple

def prepare_comment_tuple(comment,parent_object_id):
    """
    Prepare comment tuple in following format:
    (id_of_comment,created_time,updated_time,message,link,attachment_type,attachment_title and attachment_url,
    number_of_like,number_of_love,number_of_wow,number_of_haha,number_of_angry,number_of_sad,name_of_user,id_of_user)
    """
    return_tuple = ()
    return_tuple = return_tuple + (comment.get('id'),)
    return_tuple = return_tuple + (parent_object_id,)
    return_tuple = return_tuple + (datetime.datetime.strptime(comment.get('created_time'), '%Y-%m-%dT%H:%M:%S%z'),)
    if comment.get('updated_time') is None:
        return_tuple = return_tuple + (None,)
    else:
        return_tuple = return_tuple + (datetime.datetime.strptime(comment.get('updated_time'), '%Y-%m-%dT%H:%M:%S%z'),)
    return_tuple = return_tuple + (comment.get('message',''),)
    if comment.get('attachment') is None:
        return_tuple = return_tuple + ('',)
        return_tuple = return_tuple + ('',)
        return_tuple = return_tuple + ('',)
    else:
        return_tuple = return_tuple + (comment.get('attachment').get('type'),)
        return_tuple = return_tuple + (comment.get('attachment').get('title',''),)
        return_tuple = return_tuple + (comment.get('attachment').get('url',''),)
    return_tuple = return_tuple + (comment.get('react_like').get('summary').get('total_count'),)
    return_tuple = return_tuple + (comment.get('react_love').get('summary').get('total_count'),)
    return_tuple = return_tuple + (comment.get('react_wow').get('summary').get('total_count'),)
    return_tuple = return_tuple + (comment.get('react_haha').get('summary').get('total_count'),)
    return_tuple = return_tuple + (comment.get('react_angry').get('summary').get('total_count'),)
    return_tuple = return_tuple + (comment.get('react_sad').get('summary').get('total_count'),)
    return_tuple = return_tuple + (comment.get('from').get('name'),)
    return_tuple = return_tuple + (comment.get('from').get('id'),)
    return return_tuple

def get_page(page_id):
    """
    Get page info from given id
    """
    all_pages = []
    try:
        page = __graph.get_object(id=page_id,fields=','.join(__page_fields))
        data_tuple = convert_dict_tuple(page, __page_fields)
        all_pages.append(data_tuple)
    except ConnectionError as e:
        logging.info('[get_page] Error retrieving page_id %s - Message: %s', page_id, e)
        return None
    except requests.exceptions.ConnectionError as e:
            logging.info('[requests.exceptions.ConnectionError] Encountered TimeoutError in get_page for %s - Retry in %s seconds', page_id, RETRY_CONNECTION_WAIT)
            time.sleep(RETRY_CONNECTION_WAIT)
            try:
                page = __graph.get_object(id=page_id,fields=','.join(__page_fields))
                data_tuple = convert_dict_tuple(page, __page_fields)
                all_pages.append(data_tuple)
            except requests.exceptions.ConnectionError as e:
                logging.info('[requests.exceptions.ConnectionError] Encountered second TimeoutError in get_page for %s - Abort', page_id)
                return all_pages
    except facebook.GraphAPIError as e:
        logging.info('[get_page] Encountered GraphAPIError error %s - Abort', e)
        if 'Session has expired on' in str(e):
            print('[get_page] Encountered GraphAPIError error %s - Abort', e)
            exit(1)
        else:
            return all_pages
    except Exception as e:
        logging.info('[get_page] Encountered unknown error %s - Type %s - Abort', e, type(e))
        return all_pages
    return all_pages

def get_posts(page_id, limit_parameter, since_parameter=datetime.datetime.now().timetuple(), until_parameter=datetime.datetime.now().timetuple()):
    since_parameter = time.mktime(since_parameter)
    until_parameter = time.mktime(until_parameter)
    all_posts = []
    # Retrieve posts from Graph API
    try:
        posts = __graph.get_connections(id=page_id, connection_name='posts',fields=','.join(__post_fields),since=since_parameter,until=until_parameter,limit=limit_parameter)
    except ConnectionError as e:
        logging.info('[get_posts] Error retrieving id %s - Message: %s', page_id, e)
        return all_posts
    except requests.exceptions.ConnectionError  as e:
        logging.info('[requests.exceptions.ConnectionError] Encountered TimeoutError in get_posts for %s - Retry in %s seconds', page_id, RETRY_CONNECTION_WAIT)
        time.sleep(RETRY_CONNECTION_WAIT)
        try:
            posts = __graph.get_connections(id=page_id, connection_name='posts',fields=','.join(__post_fields),since=since_parameter,until=until_parameter,limit=limit_parameter)
        except requests.exceptions.ConnectionError as e:
            logging.info('[requests.exceptions.ConnectionError] Encountered second TimeoutError in get_posts for %s ', page_id)
            return all_posts
    except facebook.GraphAPIError as e:
        logging.info('[get_posts] Encountered GraphAPIError error %s', e)
        if 'Session has expired on' in str(e):
            print('[get_posts] Encountered GraphAPIError error %s - Abort extraction', e)
            exit(1)
        else:
            return all_posts
    except Exception as e:
        logging.info('[get_posts] Encountered unknown error %s - Type %s - Abort', e, type(e))
        return all_posts
    # Convert posts to defined schema and put into list
    while(True):
        try:
            logging.info('[get_posts] Start retrieving posts for page_id %s - Since: %s - Until: %s - Limit: %s', page_id,since_parameter,until_parameter,limit_parameter)
            for post in posts['data']:
                all_posts.append(prepare_post_tuple(post))
            if posts != None and posts.get('paging') != None and posts.get('paging').get('next') != None:
                posts=requests.get(posts['paging']['next']).json()
            else:
                break
        except ConnectionError as e:
            logging.info('[get_posts] Error retrieving page_id %s - Message: %s', page_id, e)
            return all_posts
        except requests.exceptions.ConnectionError as e:
            posts = retry_pagination_error(posts['paging']['next'], 'get_posts', e)
        except facebook.GraphAPIError as e:
            logging.info('[get_posts] Encountered GraphAPIError error %s - Abort', e)
            if 'Session has expired on' in str(e):
                print('[get_posts] Encountered GraphAPIError error %s - Abort extraction', e)
                exit(1)
            else:
                return all_posts
        except Exception as e:
            logging.info('[get_posts] Encountered unknown error %s - Type %s - Abort', e, type(e))
            return all_posts
    return all_posts

def get_comments(parent_object_id, limit_parameter):
    all_comments = []
    # Retrieve comments from Graph API
    try:
        comments = __graph.get_connections(id=parent_object_id, connection_name='comments',fields=','.join(__comment_fields))
    except ConnectionError as e:
        logging.info('[get_comments] Error retrieving parent_object_id %s - Message: %s', parent_object_id, e)
        return all_comments
    except requests.exceptions.ConnectionError as e:
        logging.info('[requests.exceptions.ConnectionError] Encountered TimeoutError in get_comments for %s - Retry in %s seconds', parent_object_id, RETRY_CONNECTION_WAIT)
        time.sleep(RETRY_CONNECTION_WAIT)
        try:
            comments = __graph.get_connections(id=parent_object_id, connection_name='comments',fields=','.join(__comment_fields))
        except requests.exceptions.ConnectionError as e:
            logging.info('[requests.exceptions.ConnectionError] Encountered second TimeoutError in get_comments for %s ', parent_object_id)
            return all_comments
    except facebook.GraphAPIError as e:
        if 'Session has expired on' in str(e):
            print('[get_comments] GraphAPIError error %s while retrieving %s - Abort extraction', e, parent_object_id)
            exit(1)
        else:
            logging.info('[get_comments] GraphAPIError error %s while retrieving %s', e, parent_object_id)
            return all_comments
    except Exception as e:
        logging.info('[get_comments] Unknown error %s while retrieving %s - Type %s', e,parent_object_id, type(e))
        return all_comments
    
    # Convert comments to defined schema and put into list
    while(True):
        try:
            for comment in comments['data']:
                all_comments.append(prepare_comment_tuple(comment,parent_object_id))
            if comments.get('paging') != None and comments.get('paging').get('next') != None:
                comments=requests.get(comments['paging']['next']).json()
            else:
                break
        except ConnectionError as e:
            logging.info('[requests.exceptions.ConnectionError] Encountered second TimeoutError in get_comments for %s ', parent_object_id)
            return all_comments
        except requests.exceptions.ConnectionError as e:
            comments = retry_pagination_error(comments['paging']['next'], 'get_comments', e)
        except facebook.GraphAPIError as e:
            if 'Session has expired on' in str(e):
                print('[get_comments] GraphAPIError error %s while retrieving %s - Abort extraction', e, parent_object_id)
                exit(1)
            else:
                logging.info('[get_comments] GraphAPIError error %s while retrieving %s', e, parent_object_id)
                return all_comments
        except Exception as e:
            logging.info('[get_comments] Unknown error %s while retrieving %s - Type %s', e,parent_object_id, type(e))
            return all_comments
    return all_comments


def retry_pagination_error(pagination_url, function_name, exception_message):
    logging.info('[requests.exceptions.ConnectionError] Encountered TimeoutError in %s pagination for %s - Retry in %s seconds', function_name, pagination_url, RETRY_CONNECTION_WAIT)
    time.sleep(RETRY_CONNECTION_WAIT)
    user_agent = USER_AGENT.random
    headers = {'User-Agent': user_agent}
    try:
        pagination_data = requests.get(pagination_url, headers).json()
        return pagination_data
    except requests.exceptions.ConnectionError as e:
        logging.info('[requests.exceptions.ConnectionError] Encountered second TimeoutError in %s pagination for %s - Abort', function_name, pagination_url)
        exit(1)
