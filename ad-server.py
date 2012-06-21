#ADSERVER Request Format
# http://rtbidder.impulse01.com/serve?{Base64-Encoded-Params}|||{Encrypted-Price}|||{Third-Party-Redirect-Url}
# http://rtbidder.impulse01.com/click?{Base64-Encoded-Params}|||{Redirect-Url}

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
        if self.request.path=="/conv":
		self.conversion(self.request.query)

    def serve(self,info):
	params = info.split("|||")
	args = json.loads(base64.b64decode(params[0]))
	enc_price = params[1]
	third_party_url = params[2]
	ip = self.request.remote_ip

	if adIndex.has_key('c:'+str(args['cid'])+':b:'+str(args['bid'])):
		url = adIndex['c:'+str(args['cid'])+':b:'+str(args['bid'])]
	else : 
		url = adIndex['c:'+str(args['cid'])+':url']

	if len(third_party_url)==0:
		finalUrl="http://ec2-175-41-181-197.ap-southeast-1.compute.amazonaws.com/click?"+params[0]+"|||"+url
	else:
		finalUrl=third_party_url+urllib.quote("http://ec2-175-41-181-197.ap-southeast-1.compute.amazonaws.com/click?"+params[0]+"|||"+url)

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
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
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
	self.set_header("Location",redirect_url)
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

	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='clicks')
	channel.basic_publish(exchange='',routing_key='clicks',body=message)
	connection.close()

    def segment(self,info):
	self.write("segment")
	self.write(info)
	self.write("<br />")

    def sync(self,info):
	self.write("sync")
	self.write(info)
	self.write("<br />")

    def conversion(self,info):
	self.write("conversion")
	self.write(info)
	self.write("<br />")

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
