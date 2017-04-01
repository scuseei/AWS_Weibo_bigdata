# -*- coding: utf-8 -*-
"""
Created on Tue Aug 09 12:21:28 2016

@author: chenshus
"""

from weibo import APIClient
import webbrowser
import time
import numpy as np
import logging
import boto3
import json

###############################################################################
# set logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')

###############################################################################
class weibo_API_access():
    
    def __init__(self, APP_KEY, APP_SECRET, CALLBACK_URL):
		self.APP_KEY = APP_KEY
		self.APP_SECRET = APP_SECRET
		self.CALLBACK_URL = CALLBACK_URL    
    
    # get client access
    def get_client_access(self):
        #请求用户授权的过程
        self.client = APIClient(self.APP_KEY, self.APP_SECRET)
        authorize_url = self.client.get_authorize_url(self.CALLBACK_URL)
        #打开浏览器，需手动找到地址栏中URL里的code字段
        webbrowser.open(authorize_url)
        #将code字段后的值输入控制台中
        code = raw_input("input the code: ").strip()
        #获得用户授权
        request = self.client.request_access_token(code, CALLBACK_URL)
        #保存access_token ,exires_in, uid
        access_token = request.access_token
        expires_in = request.expires_in
        #uid = request.uid
        #设置accsess_token，client可以直接调用API了
        self.client.set_access_token(access_token, expires_in)
        logging.info('client successfully accessed')
        return self.client

###############################################################################
class weibo_crawling():
    
    def __init__(self, client, API_using_duration, API_using_pause):
        self.client = client
        self.API_using_duration = API_using_duration
        self.API_using_pause = API_using_pause         
         
    # get weibo content, return 
    def get_weibo_content(self):
        #不同的API，实现不同的抓取
        #get_results = self.client.statuses__mentions()
        #get_results = self.client.frientdships__friends__ids()
        #get_results = self.client.statuses__user_timeline()
        #get_results = self.client.statuses__repost_timeline(id = uid)
        #get_results = self.client.search__topics(q = "笨NANA")
        #get_results = self.client.statuses__friends_timeline()
        get_results = self.client.statuses__public_timeline(count = 199)
        #get_results = self.client.statuses__user_timeline()
        get_statuses = get_results.__getattr__('statuses')
        return get_statuses
      
    # uninterrupted get weubo content return
    def uninterruptedly_get_weibo_content(self):
        N = np.floor(self.API_using_duration/self.API_using_pause)
        weibo_list = weibo_crawling.get_weibo_content(self)
        time.sleep(self.API_using_pause)
        i = 1
        print 'using API '+ str(i) +' time'
        while i < N:
            weibo_list.extend(weibo_crawling.get_weibo_content(self))
            time.sleep(self.API_using_pause)
            i = i + 1
            print 'using API '+ str(i) +' times'
        return weibo_list
     
    # set outcome of this class
    def outcome(self):
        weibo_list = weibo_crawling.uninterruptedly_get_weibo_content(self)
        return weibo_list
        
###############################################################################
class save_to_s3():
    
    def __init__(self, bucket_name, file_tobe_saved, file_name):
        self.bucket_name = bucket_name
        self.file_tobe_saved = file_tobe_saved
        self.file_name = file_name
    
    # create s3 bucket
    def s3_connection(self):
        self.s3 = boto3.resource('s3')
        self.s3.create_bucket(Bucket = self.bucket_name)
        logging.info('s3 bucket created') 
    
    # save file to s3
    def s3_save(self):
        save_to_s3.s3_connection(self)
        with open(file_name, 'w') as f:
            json.dump(self.file_tobe_saved, f)
        self.s3.Object(self.bucket_name, file_name).put(Body=open(file_name, 'rb'))
        logging.info('weibo file saved to s3') 
        
###############################################################################
if __name__ == "__main__":
    
    # information
    logging.info('program started') 
    logging.info('you should get access to AWS already via CLI') 
    
    # Class: get sina weibo client access
    APP_KEY = 'XXXXXX' # app key
    APP_SECRET = 'YYYYYY' # app secret
    CALLBACK_URL = 'https://api.weibo.com/oauth2/default.html' # callback url
    ac = weibo_API_access(APP_KEY, APP_SECRET, CALLBACK_URL)
    access = ac.get_client_access()
    
    # Class: get weibo content in JSON
    API_using_duration = 3600 # seconds
    API_using_pause = 35 # seconds
    wc = weibo_crawling(access, API_using_duration, API_using_pause)
    weibo_list = wc.outcome()
    
    # Class: save weibo content to s3
    bucket_name = 'wrk301sinaweibo'
    file_name = 'weibo_list1.json'
    ss3 = save_to_s3(bucket_name, weibo_list, file_name)
    ss3.s3_save()
    
