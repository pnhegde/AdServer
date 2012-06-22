#ADSERVER Request Format
# IMPRESSION - http://rtbidder.impulse01.com/serve?{Base64-Encoded-Params}|||{Encrypted-Price}|||{Third-Party-Redirect-Url}
# CLICK - http://rtbidder.impulse01.com/click?{Base64-Encoded-Params}|||{Redirect-Url}
# SEGMENT - http://rtbidder.impulse01.com/segment?group={GroupId}

import tornado.ioloop
import tornado.web
import tornado.httpclient
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
from urlparse import urlparse
from tornado.web import asynchronous
import tornado.options
from tornado.options import define, options

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        if self.request.path=="/serve":
		self.serve(self.request.query)
        if self.request.path=="/click":
		self.click(self.request.query)
        if self.request.path=="/segment":
		self.segment(self.request.query)
        if self.request.path=="/sync":
		self.sync(self.request.query)
        if self.request.path=="/pixel":
		self.pixel(self.request.query)
        if self.request.path=="/conv":
		self.conversion(self.request.query)

    def serve(self,info):
	params = info.split("|||")
	args = json.loads(base64.b64decode(params[0]))
	enc_price = params[1]
	third_party_url = params[2]
	ip = self.request.remote_ip

	if adIndex.has_key('c:'+str(args['cid'])+':b:'+str(args['bid'])+':url'):
		url = adIndex['c:'+str(args['cid'])+':b:'+str(args['bid'])+':url']
	else : 
		url = adIndex['c:'+str(args['cid'])+':url']

	if len(third_party_url)==0:
		finalUrl="http://rtbidder.impulse01.com/click?"+params[0]+"|||"+url
	else:
		finalUrl=third_party_url+urllib.quote("http://rtbidder.impulse01.com/click?"+params[0]+"|||"+url)

	creativeUrl = adIndex['b:'+str(args['bid'])+':url']
	bannerData = adIndex['b:'+str(args['bid'])+':data']

	if bannerData[0]==1:
		self.write('<a href="'+finalUrl+'" target="_blank"><img src="http://d3pim9r6015lw5.cloudfront.net'+creativeUrl+'" width="'+str(bannerData[1])+'" height="'+str(bannerData[2])+'" ></a>')

	if bannerData[0]==2:
		self.write('<object classid="clsid:d27cdb6e-ae6d-11cf-96b8-444553540000" codebase="http://download.macromedia.com/pub/shockwave/cabs/flash/swflash.cab#version=6,0,40,0" width="'+str(bannerData[1])+'" height="'+str(bannerData[2])+'"  id="mymoviename"><param name="movie" value="http://d3pim9r6015lw5.cloudfront.net'+creativeUrl+'?clickTag='+urllib.quote(finalUrl)+'" /> <param name="quality" value="high" /> <param name="bgcolor" value="#ffffff" /> <embed src="http://d3pim9r6015lw5.cloudfront.net'+creativeUrl+'?clickTag='+urllib.quote(finalUrl)+'" quality="high" bgcolor="#ffffff"width="'+str(bannerData[1])+'" height="'+str(bannerData[2])+'" name="mymoviename" align="" type="application/x-shockwave-flash" pluginspage="http://www.macromedia.com/go/getflashplayer"> </embed> </object>')

	if bannerData[0]==3:
		finalUrl=third_party_url+urllib.urlencode("http://rtbidder.impulse01.com/click?params="+params[0]+"&redirect=")
		code=adIndex['b:'+str(args['bid'])+':code']
		code.replace("[CLICK_MACRO]",urllib.urlencode(finalUrl))
		self.write(code)

	self.flush()

        log={"message":"IMP",
        "campaignId":str(args['cid']),
        "bannerId":str(args['bid']),
        "exchange":str(args['e']),
        "domain":str(args['d']),
	"price":enc_price,
	"timestamp":datetime.date.today().strftime("%Y-%d-%m %H:%M:%S"),
	"clickUrl":finalUrl,
	"ip":ip
        }

	message=json.dumps(log)

	#Push this impression to rabbitMQ for logging
	credentials = pika.PlainCredentials('guest', 'appyfizz')
	connection = pika.BlockingConnection(pika.ConnectionParameters(credentials=credentials, host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='imps')
	channel.basic_publish(exchange='',routing_key='imps',body=message)
	connection.close()


    def click(self,info):
	params = info.split("|||")
	args = json.loads(base64.b64decode(params[0]))
	redirect_url = params[1]

        cookieval=base64.b64encode(json.dumps({"campaignId":str(args['cid']),
        "bannerId":str(args['bid']),
        "exchange":str(args['e']),
        "domain":str(args['d']),
	"timestamp":datetime.date.today().strftime("%Y-%d-%m %H:%M:%S")
        }))
	cookiename = 'c'+str(args['cid'])
	self.set_cookie(cookiename,cookieval,expires_days=30)
	self.redirect(redirect_url)
	self.flush()

        log={"message":"CLK",
        "campaignId":str(args['cid']),
        "bannerId":str(args['bid']),
        "exchange":str(args['e']),
        "domain":str(args['d']),
	"timestamp":datetime.date.today().strftime("%Y-%d-%m %H:%M:%S"),
	"clickUrl":redirect_url,
        }
	message=json.dumps(log)

	#Push this click to rabbitMQ for logging
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='clicks')
	channel.basic_publish(exchange='',routing_key='clicks',body=message)
	connection.close()

    def segment(self,info):
	try:
		group=int(self.get_argument('group'))
		imp_uid=self.get_cookie("imp_uid",default=False)
		if imp_uid==False:
			imp_uid=str(uuid.uuid4())
			self.set_cookie("imp_uid",imp_uid)
			self.write("document.write(\"<img width='1' height='1' src='http://r.openx.net/set?pid=532485e2-f94e-8ad2-384a-01d3e0cdd7f1&rtb="+imp_uid+"'>\");\n")
			self.write("document.write(\"<script src='http://ec2-175-41-181-197.ap-southeast-1.compute.amazonaws.com/pixel?group="+str(group)+"'></script>\");\n")
			self.write("document.write(\"<img width='1' height='1' src='http://ec2-175-41-181-197.ap-southeast-1.compute.amazonaws.com/sync'>\");\n")
			self.flush()
			message_newuser=json.dumps({"message":"NEWUSER",
			"imp_uid":imp_uid
			})
			message_adduser=json.dumps({"message":"ADDUSER",
			"imp_uid":imp_uid,
			"group":group
			})
			connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
			channel = connection.channel()
			channel.queue_declare(queue='audience')
			channel.basic_publish(exchange='',routing_key='audience',body=message_newuser)
			channel.basic_publish(exchange='',routing_key='audience',body=message_adduser)
			connection.close()
		else :
			sy=self.get_cookie("sy",default=False)
			if sy==False:
				self.write("document.write(\"<img width='1' height='1' src='http://r.openx.net/set?pid=532485e2-f94e-8ad2-384a-01d3e0cdd7f1&rtb="+imp_uid+"'>\");\n")
				self.write("document.write(\"<script src='http://ec2-175-41-181-197.ap-southeast-1.compute.amazonaws.com/pixel?group="+str(group)+"'></script>\");\n")
				self.write("document.write(\"<img width='1' height='1' src='http://ec2-175-41-181-197.ap-southeast-1.compute.amazonaws.com/sync'>\");\n")
			self.flush()
			message_adduser=json.dumps({"message":"ADDUSER",
			"imp_uid":imp_uid,
			"group":group
			})
			connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
			channel = connection.channel()
			channel.queue_declare(queue='audience')
			channel.basic_publish(exchange='',routing_key='audience',body=message_adduser)
			connection.close()
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
		self.write("<script src=\"http://i.simpli.fi/dpx.js?cid=1565&action=100&segment=Impulse_segment_"+str(group)+"&m=1\"></script>")
	except:
		print "pixel exception"

    def conversion(self,info):
	try:
		campaignId=int(self.get_argument('id'))
		cookiename="c"+campaignId
		clickinfo=self.get_cookie(cookiename,default=False)
		if clickinfo!=False:
			args=json.loads(base64.b64decode(clickinfo))
			log={"message":"CONV",
			"campaignId":str(args['campaignId']),
			"bannerId":str(args['bannerId']),
			"exchange":str(args['exchange']),
			"domain":str(args['domain']),
			"timestamp":datetime.date.today().strftime("%Y-%d-%m %H:%M:%S"),
			}
			message=json.dumps(log)
			connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
			channel = connection.channel()
			channel.queue_declare(queue='conversions')
			channel.basic_publish(exchange='',routing_key='conversions',body=message)
			connection.close()
	except:
		print "conversion exception"

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
