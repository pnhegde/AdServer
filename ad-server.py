#ADSERVER Request Format
# IMPRESSION - http://rtbidder.impulse01.com/serve?{Base64-Encoded-Params}|||{Encrypted-Price}|||{Third-Party-Redirect-Url}
# CLICK - http://rtbidder.impulse01.com/click?{Base64-Encoded-Params}|||{Redirect-Url}
# SEGMENT - http://rtbidder.impulse01.com/segment?group={GroupId}

import random
import time  
import hashlib
import re
import json
import datetime
import base64

import pika
import urllib
import uuid
import tornado.ioloop
import tornado.web
import tornado.httpclient
import redis

from urlparse import urlparse
from tornado.web import asynchronous
import tornado.options
from tornado.options import define, options

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        if self.request.path == "/serve":
            self.serve(self.request.query)
        if self.request.path == "/click":
            self.click(self.request.query)
        if self.request.path == "/segment":
            self.segment(self.request.query)
        if self.request.path == "/sync":
            self.sync(self.request.query)
        if self.request.path == "/pixel":
            self.pixel(self.request.query)
        if self.request.path == "/conv":
            self.conversion(self.request.query)
        if self.request.path == "/healthcheck":
            self.healthcheck(self.request.query)

    def serve(self,info):
        params = self.get_argument('info')
        newParams = params.replace("-","+").replace("_","/")
        newParams = newParams + '=' * (4 - len(newParams) % 4)
        args = json.loads(base64.b64decode(newParams))
        encrPrice = self.get_argument('p')
        random = self.get_argument('r')

        #Here we assume that the third party URL being passed is not URL Escaped. Hence split by &red=
        ta = self.request.query.split("&red=")
        thirdPartyUrl = ta[1]
        ip = self.request.remote_ip

        if adIndex.has_key('c:'+str(args['cid'])+':b:'+str(args['bid'])+':url'):
            url = adIndex['c:'+str(args['cid'])+':b:'+str(args['bid'])+':url']
        else : 
            url = adIndex['c:'+str(args['cid'])+':url']

        if len(thirdPartyUrl) == 0:
            finalUrl = "http://rtbidder.impulse01.com/click?info="+params+"&red="+url
        else:
            finalUrl = thirdPartyUrl+urllib.quote("http://rtbidder.impulse01.com/click?info="+params+"&red="+url)

        creativeUrl = adIndex['b:'+str(args['bid'])+':url']
        bannerData = adIndex['b:'+str(args['bid'])+':data']

        if bannerData[0] == 1:
            self.write('<a href="'+finalUrl+'" target="_blank"><img src="http://d3pim9r6015lw5.cloudfront.net'+creativeUrl+'" width="'+str(bannerData[1])+'" height="'+str(bannerData[2])+'" ></a>')
 
        if bannerData[0] == 2:
            self.write('<object classid="clsid:d27cdb6e-ae6d-11cf-96b8-444553540000" codebase="http://download.macromedia.com/pub/shockwave/cabs/flash/swflash.cab#version=6,0,40,0" width="'+str(bannerData[1])+'" height="'+str(bannerData[2])+'"  id="mymoviename"><param name="movie" value="http://d3pim9r6015lw5.cloudfront.net'+creativeUrl+'?clickTag='+urllib.quote(finalUrl)+'" /> <param name="quality" value="high" /> <param name="bgcolor" value="#ffffff" /><param name="wmode" value="transparent"><embed src="http://d3pim9r6015lw5.cloudfront.net'+creativeUrl+'?clickTag='+urllib.quote(finalUrl)+'" quality="high" bgcolor="#ffffff" width="'+str(bannerData[1])+'" height="'+str(bannerData[2])+'" name="mymoviename" align="" type="application/x-shockwave-flash" pluginspage="http://www.macromedia.com/go/getflashplayer"> </embed> </object>')

        if bannerData[0] == 3:
            finalUrl = thirdPartyUrl+urllib.urlencode("http://rtbidder.impulse01.com/click?info="+params+"&red=")
            code = adIndex['b:'+str(args['bid'])+':code']
            code.replace("[CLICK_MACRO]",urllib.urlencode(finalUrl))
            self.write(code)

        if args.has_key('piggyback'):
            pb=args['piggyback']
            for p in pb:
                self.write("<script src=\"http://rtbidder.impulse01.com/segment?group="+str(p)+"\"></script>")

        if args['cid']==113:
            self.write("<script src=\"http://rtbidder.impulse01.com/segment?group=21\"></script>")

        self.flush()

        message=json.dumps({"message":"IMP",
            "campaignId":args['cid'],
            "bannerId":args['bid'],
            "exchange":args['e'],
            "domain":args['d'],
            "price":encrPrice,
            "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S"),
            "clickUrl":finalUrl,
            "ip":ip
        })
        self.sendtoredis('imps',message)

    def click(self,info):
        params = self.get_argument('info')
        newParams = params.replace("-","+").replace("_","/")
        newParams = newParams + '=' * (4 - len(newParams) % 4)
        args = json.loads(base64.b64decode(newParams))

        ta=self.request.query.split("&red=")
        redirect_url = ta[1]

        cookieval = base64.b64encode(json.dumps({"campaignId":args['cid'],
            "bannerId":args['bid'],
            "exchange":args['e'],
            "domain":args['d'],
            "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S")
        }))
        cookiename = 'c'+str(args['cid'])
        self.set_cookie(cookiename,cookieval,expires_days=30)
        self.redirect(redirect_url)

        log = {"message":"CLK",
            "campaignId":args['cid'],
            "bannerId":args['bid'],
            "exchange":args['e'],
            "domain":args['d'],
            "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S"),
            "clickUrl":redirect_url
        }
        message=json.dumps(log)
        self.sendtoredis('clicks',message)

    def segment(self,info):
        try:
            group = int(self.get_argument('group'))
            queryString = self.request.query    #get query string of the url
            attributes = dict([part.split('=') for part in queryString.split('&')]) #Convert the query to dictonary
            del attributes['group'] #Remove the 1st argument 'group'
                                            
            imp_uid = self.get_cookie("imp_uid",default=False)
            if imp_uid == False:
                imp_uid = str(uuid.uuid4())
                self.set_cookie("imp_uid",imp_uid,expires_days=365)
                self.write("document.write(\"<img width='1' height='1' src='http://r.openx.net/set?pid=532485e2-f94e-8ad2-384a-01d3e0cdd7f1&rtb="+imp_uid+"'>\");\n")
                self.write("document.write(\"<script src='http://rtbidder.impulse01.com/pixel?group="+str(group)+"'></script>\");\n")
                self.write("document.write(\"<img width='1' height='1' src='http://rtbidder.impulse01.com/sync'>\");\n")
                self.flush()
                
                #Check the url query for attributes
                if len(attributes) != 0 :
                    attributeJson = json.dumps(attributes) #Convert dictonary to JSON before sending it to redis
                    message_adduserattr = json.dumps({"message":"ADDUSERATTR",
                                                  "imp_uid":imp_uid,
                                                  "group":group,
                                                  "attribute":attributeJson
                                                  })
                    self.sendtoredis('audience', message_adduserattr)
                else : 
                    message_adduser = json.dumps({"message":"ADDUSER",
                                              "imp_uid":imp_uid,
                                              "group":group
                                              })
                    self.sendtoredis('audience', message_adduser)    
                
            else :
                sy = self.get_cookie("sy",default=False)
                if sy == False:
                    self.write("document.write(\"<img width='1' height='1' src='http://r.openx.net/set?pid=532485e2-f94e-8ad2-384a-01d3e0cdd7f1&rtb="+imp_uid+"'>\");\n")
                    self.write("document.write(\"<script src='http://rtbidder.impulse01.com/pixel?group="+str(group)+"'></script>\");\n")
                    self.write("document.write(\"<img width='1' height='1' src='http://rtbidder.impulse01.com/sync'>\");\n")
                self.flush()
                
                if len(attributes) != 0 :
                    attributeJson = json.dumps(attributes)
                    message_adduserattr = json.dumps({"message":"ADDUSERATTR",
                                                  "imp_uid":imp_uid,
                                                  "group":group,
                                                  "attribute":attributeJson
                                                  })
                    self.sendtoredis('audience', message_adduserattr)
                else :
                    message_adduser = json.dumps({"message":"ADDUSER",
                                                  "imp_uid":imp_uid,
                                                  "group":group
                                                  })
                    self.sendtoredis('audience',message_adduser)
        except:
            print "segment exception"

    def sync(self,info):
        print "sync request"
        self.set_cookie("sy","yes",expires_days=30)
        self.set_header("Content-type","image/gif")
        #NOTE - This is the binary of a 1x1 gif pixel in base64 encoded form
        self.write(base64.b64decode("R0lGODlhAQABAIAAAP///wAAACH5BAAAAAAALAAAAAABAAEAAAICRAEAOw=="))

    def pixel(self,info):
        try:
            group=int(self.get_argument('group'))
            self.write("document.write(\"<script src='http://i.simpli.fi/dpx.js?cid=1565&action=100&segment=Impulse_segment_"+str(group)+"&m=1'></script>\");\n")
        except:
            print "pixel exception"

    def conversion(self,info):
        try:
            campaignId=int(self.get_argument('id'))
            cookiename="c"+campaignId
            clickinfo=self.get_cookie(cookiename,default=False)
            if clickinfo!=False:
                args=json.loads(base64.b64decode(clickinfo))
                message=json.dumps({"message":"CONV",
                    "campaignId":args['campaignId'],
                    "bannerId":args['bannerId'],
                    "exchange":args['exchange'],
                    "domain":args['domain'],
                    "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S")
                })
                if adIndex.has_key('c:'+campaignId+':sifi'):
                    sifiid=adIndex['c:'+campaignId+':sifi']
                    self.write("document.write(\"<script src='http://i.simpli.fi/dpx.js?cid=1565&conversion=0&campaign_id="+sifiid+"&m=1&c=0'></script>\");\n")
                self.sendtoredis('conversions',message)
            if campaignId==115:
                self.write("document.write(\"<script src='http://i.simpli.fi/dpx.js?cid=1565&conversion=10&campaign_id=8683&m=1'></script>\");\n")

        except:
            print "conversion exception"

    def healthcheck(self,info):
        self.write("i am ok")

    def sendtorabbit(self,qname,msg):
        credentials = pika.PlainCredentials('guest', 'appyfizz')
        connection = pika.BlockingConnection(pika.ConnectionParameters(credentials=credentials, host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=qname)
        channel.basic_publish(exchange='',routing_key=qname,body=msg)
        connection.close()

    def sendtoredis(self,qname,msg):
        r = redis.StrictRedis(host='localhost', port=6379, db=0)
        r.lpush('globalqueue',msg)

def refreshCache():
    global adIndex
    http_client = tornado.httpclient.HTTPClient()
    try:
        response = http_client.fetch("http://user.impulse01.com/ad-index.php")
        invertedIndex=json.loads(response.body)
    except:
        invertedIndex=dict()
    adIndex=invertedIndex
    

define("port", default=8888, help="run on the given port", type=int)
define("name", default="noname", help="name of the server")
define("refreshCache", default=5000, help="millisecond interval between cache refresh", type=int)

application = tornado.web.Application([(r".*", MainHandler),])
adIndex = dict()

if __name__ == "__main__":
    tornado.options.parse_command_line()
    application.listen(options.port)
    tornado.ioloop.PeriodicCallback(refreshCache, options.refreshCache).start()
    tornado.ioloop.IOLoop.instance().start()
