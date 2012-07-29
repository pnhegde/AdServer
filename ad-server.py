# ADSERVER Request Format
# IMPRESSION - http://rtbidder.impulse01.com/serve?{Base64-Encoded-Params}|||{Encrypted-Price}|||{Third-Party-Redirect-Url}
# CLICK - http://rtbidder.impulse01.com/click?{Base64-Encoded-Params}|||{Redirect-Url}
# SEGMENT - http://rtbidder.impulse01.com/segment?group={GroupId}
# Copyright - Impulse Media Pvt. Ltd.

from random import choice
import time
import hashlib
import re
import json
import datetime
import base64
import sys

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
        if self.request.path == "/conversion":
            self.conversion(self.request.query)
        if self.request.path == "/healthcheck":
            self.healthcheck(self.request.query)
        if self.request.path == "/google_match":
            self.google_match(self.request.query)
        if self.request.path == "/vast_imp":
            self.vast_imp(self.request.query)    
        if self.request.path == "/optout":
            self.optout(self.request.query)  
            
    def serve(self,info):
        params = self.get_argument('info')
        newParams = params.replace("-","+").replace("_","/")
        newParams = newParams + '=' * (4 - len(newParams) % 4)
        args = json.loads(base64.b64decode(newParams))
        encrPrice = self.get_argument('p')
        random = self.get_argument('r')

        #No banner ID passed. This means this is a direct ad code. Whatever it is, decide the banner first and update the arguments.
        if args['bid']==0:
	    banners = adIndex['banners:'+str(args['cid'])+':'+str(args['w'])+':'+str(args['h'])]
	    randomBannerId = choice(banners)
	    args['bid']=randomBannerId
	    
	if !args.has_key['user_geo_state']:
	    args['user_geo_state']="NA"
	
	if !args.has_key['user_geo_dma']:
	    args['user_geo_dma']="NA"	
	    
        #Here we assume that the third party URL being passed is not URL Escaped. Hence split by &red=
        ta = self.request.query.split("&red=")
        thirdPartyUrl = ta[1]
        ip = self.request.remote_ip
        
        #If imp_uid is not set, we are seeing the user for first time.
        #Create new cookie imp_uid and also drop syncronization pixels in the browser
        imp_uid = self.get_cookie("imp_uid",default=False)
        if imp_uid == False:
	    imp_uid = str(uuid.uuid4())
	    self.set_cookie("imp_uid",imp_uid,expires_days=365)
	    self.write("<img width='1' height='1' src='http://r.openx.net/set?pid=532485e2-f94e-8ad2-384a-01d3e0cdd7f1&rtb="+imp_uid+"'>\n")
            self.write("<img width='1' height='1' src='http://rtbidder.impulse01.com/sync'>\n")
            self.write("<img width='1' height='1' src='http://cm.g.doubleclick.net/pixel?google_nid=ipm&google_cm'>\n")
        else:
            sy2 = self.get_cookie("sy2",default=False)
            if sy2 == False:
		self.write("<img width='1' height='1' src='http://r.openx.net/set?pid=532485e2-f94e-8ad2-384a-01d3e0cdd7f1&rtb="+imp_uid+"'>\n")
		self.write("<img width='1' height='1' src='http://rtbidder.impulse01.com/sync'>\n")
		self.write("<img width='1' height='1' src='http://cm.g.doubleclick.net/pixel?google_nid=ipm&google_cm'>\n")

	args['imp_uid']=imp_uid
	impressionId=str(uuid.uuid4())
	args['impressionId']=impressionId
	params=base64.b64encode(json.dumps(args))
	params=params.replace("+","-").replace("/","_").replace("=","")

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

        #Set the view through cookie to indicate that this user has seen this ad impression.
        #View through cookies are in the form of i203 where 203= campaign ID
        cookieval = base64.b64encode(json.dumps({"cid":args['cid'],
            "bid":args['bid'],
            "e":args['e'],
            "d":args['d'],
            "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S")
        }))
        cookiename = 'v'+str(args['cid'])
        if adIndex.has_key('vw:'+str(args['cid'])):
            vw = adIndex['vw:'+str(args['cid'])]
            if vw==0:
	        vw=30
        else : 
            vw=30
        self.set_cookie(cookiename,cookieval,expires_days=vw)
        
        self.set_header("Cache-Control","no-cache")
        self.set_header("Pragma","no-cache")
        self.flush()

        message=json.dumps({"message":"IMP",
            "impressionId":impressionId,
            "imp_uid":imp_uid,
            "campaignId":args['cid'],
            "bannerId":args['bid'],
            "exchange":args['e'],
            "domain":args['d'],
            "state":args['user_geo_state'],
            "dma":args['user_geo_dma'],
            "price":encrPrice,
            "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S"),
        })
        self.sendToLogAgent(message)

    def click(self,info):
        params = self.get_argument('info')
        newParams = params.replace("-","+").replace("_","/")
        newParams = newParams + '=' * (4 - len(newParams) % 4)
        args = json.loads(base64.b64decode(newParams))

        ta=self.request.query.split("&red=")
        redirect_url = ta[1]

        #Set the click through cookie to indicate that the user clicked on this ad.
        cookieval = base64.b64encode(json.dumps({"cid":args['cid'],
            "bid":args['bid'],
            "e":args['e'],
            "d":args['d'],
            "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S")
        }))
        cookiename = 'c'+str(args['cid'])
        if adIndex.has_key('cw:'+str(args['cid'])):
            cw = adIndex['cw:'+str(args['cid'])]
            if cw==0:
	        cw=30
        else :
            cw=30        
        self.set_cookie(cookiename,cookieval,expires_days=cw)
        self.redirect(redirect_url)
        
        imp_uid = self.get_cookie("imp_uid",default=False)
               
        log = {"message":"CLK",
            "imp_uid":imp_uid,
            "impressionId":args['impressionId'],
            "campaignId":args['cid'],
            "bannerId":args['bid'],
            "exchange":args['e'],
            "domain":args['d'],
            "timestamp_GMT":datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S"),
        }
        message=json.dumps(log)
        self.sendToLogAgent(message)

    def segment(self,info):
        try:
            group = int(self.get_argument('group'))
            if group==24:
	      self.write("document.write(\"<img height='1' width='1' src='http://www.googleadservices.com/pagead/conversion/952217567/?value=0&amp;label=GHfnCMm2owQQ39-GxgM&amp;guid=ON&amp;script=0'/>\");")
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
                self.write("document.write(\"<img width='1' height='1' src='http://cm.g.doubleclick.net/pixel?google_nid=ipm&google_cm'>\");\n")
                self.flush()
                
                #Check the url query for attributes
                if len(attributes) != 0 :
                    attributeJson = json.dumps(attributes) #Convert dictonary to JSON before sending it to redis
                    message_adduserattr = json.dumps({"message":"ADDUSERATTR",
                                                  "imp_uid":imp_uid,
                                                  "group":group,
                                                  "attribute":attributeJson
                                                  })
                    self.sendToLogAgent(message_adduserattr)
                else : 
                    message_adduser = json.dumps({"message":"ADDUSER",
                                              "imp_uid":imp_uid,
                                              "group":group
                                              })
                    self.sendToLogAgent(message_adduser)
                
            else :
                sy2 = self.get_cookie("sy2",default=False)
                if sy2 == False:
                    self.write("document.write(\"<img width='1' height='1' src='http://r.openx.net/set?pid=532485e2-f94e-8ad2-384a-01d3e0cdd7f1&rtb="+imp_uid+"'>\");\n")
                    self.write("document.write(\"<script src='http://rtbidder.impulse01.com/pixel?group="+str(group)+"'></script>\");\n")
                    self.write("document.write(\"<img width='1' height='1' src='http://rtbidder.impulse01.com/sync'>\");\n")
                    self.write("document.write(\"<img width='1' height='1' src='http://cm.g.doubleclick.net/pixel?google_nid=ipm&google_cm'>\");\n")
                self.flush()
                
                if len(attributes) != 0 :
                    attributeJson = json.dumps(attributes)
                    message_adduserattr = json.dumps({"message":"ADDUSERATTR",
                                                  "imp_uid":imp_uid,
                                                  "group":group,
                                                  "attribute":attributeJson
                                                  })
                    self.sendToLogAgent(message_adduserattr)
                else :
                    message_adduser = json.dumps({"message":"ADDUSER",
                                                  "imp_uid":imp_uid,
                                                  "group":group
                                                  })
                    self.sendToLogAgent(message_adduser)
        except:
            print "segment exception",sys.exc_info()

    def sync(self,info):
        self.set_cookie("sy2","yes",expires_days=30)
        self.set_header("Content-Type","image/gif")
        #NOTE - This is the binary of a 1x1 gif pixel in base64 encoded form
        self.write(base64.b64decode("R0lGODlhAQABAIAAAP///////yH+EUNyZWF0ZWQgd2l0aCBHSU1QACwAAAAAAQABAAACAkQBADs="))

    def pixel(self,info):
        try:
            group=int(self.get_argument('group'))
            self.write("document.write(\"<script src='http://i.simpli.fi/dpx.js?cid=1565&action=100&segment=Impulse_segment_"+str(group)+"&m=1'></script>\");\n")
        except:
            print "pixel exception",sys.exc_info()

    def conversion(self,info):
        try:
            campaignId=int(self.get_argument('id'))
            cookiename="c"+str(campaignId)
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
                if adIndex.has_key('c:'+str(campaignId)+':sifi'):
                    sifiid=adIndex['c:'+str(campaignId)+':sifi']
                    self.write("document.write(\"<script src='http://i.simpli.fi/dpx.js?cid=1565&conversion=0&campaign_id="+str(sifiid)+"&m=1&c=0'></script>\");\n")
                self.sendToLogAgent(message)

            if campaignId==115:
                self.write("document.write(\"<script src='http://i.simpli.fi/dpx.js?cid=1565&conversion=10&campaign_id=8683&m=1'></script>\");\n")
                print "stuff"
        except:
            print "conversion exception",sys.exc_info()

    def convert(self,info):
        print "convered"
        
    def healthcheck(self,info):
        self.write("i am ok")
        
    def google_match(self,info):
        imp_uid = self.get_cookie("imp_uid",default=False)
        google_gid = self.get_argument('google_gid')
        message_googlematch = json.dumps({"message":"GOOGLEMATCH",
                              "imp_uid":imp_uid,
                              "google_gid":google_gid
                          })
        self.sendToLogAgent(message_googlematch)                    
        #NOTE - This is the binary of a 1x1 gif pixel in base64 encoded form
        self.set_header("Content-Type","image/gif")
        self.write(base64.b64decode("R0lGODlhAQABAIAAAP///////yH+EUNyZWF0ZWQgd2l0aCBHSU1QACwAAAAAAQABAAACAkQBADs="))

    def vast_imp(self,info):
        params = self.get_argument('info')
        newParams = params.replace("-","+").replace("_","/")
        newParams = newParams + '=' * (4 - len(newParams) % 4)
        args = json.loads(base64.b64decode(newParams))
        message=json.dumps({"message":"IMP",
            "campaignId":args['cid'],
            "bannerId":args['bid'],
            "exchange":args['e'],
            "domain":args['d']
            })
        self.sendToLogAgent(message)
        
    def optout(self,info):
        self.clear_all_cookies()
        self.write("You have been opted out and we can no longer track you")
    
    def sendToLogAgent(self,message):
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch('http://localhost:9000/access', method='POST',body=message,callback=None)

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
    print "starting server name="+options.name
    print "refreshing cache first time"
    refreshCache()
    tornado.options.parse_command_line()
    application.listen(options.port)
    tornado.ioloop.PeriodicCallback(refreshCache, options.refreshCache).start()
    tornado.ioloop.IOLoop.instance().start()
