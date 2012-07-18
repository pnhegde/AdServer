#!/usr/bin/env python

import tornado.ioloop
import tornado.web
import tornado.options

import uuid
import os
import threading

from os import listdir
from os.path import isfile, join
from logging.config import fileConfig
from tornado.options import define, options

class MainHandler(tornado.web.RequestHandler):
    
    def post(self):
        if self.request.path == "/access":
            self.access()
        if self.request.path == "/poll":
            self.poll()
        if self.request.path == "/getFile":
            self.getFile()
    
    def access(self):
        global timeout
        global logList
        logMsg = self.get_argument('log')
        if len(logList) < 5000 : 
            logList.append(str(logMsg))
        elif len(logList) == 5000 :
            i = 1
            try:
                print "Creating file" + str(i)
                i += 1
                f = open(logFolder+'/'+str(uuid.uuid4()),'w')
                f.write(str(logList))
                logList = []
                timeout = False
                f.close()
            except:
                print "File create error in access"
        
    def poll(self):
        try:
            allFiles = [ f for f in listdir(logFolder) if isfile(join(logFolder,f)) ]
            if allFiles:
                self.write(allFiles)
        except:
            print "List file error" 
              
    def getFile(self):
        try:
            fileName = str(self.get_argument('file'))
            if filename:
                fileContent = open(fileName,'r').read()
                self.write(fileContent)
                
        except:
            print "file open error"                
    #timeoutFunction()

def timeoutFunction():
    global timeout
      if timeout:
        try:
            f = open(logFolder+'/'+str(uuid.uuid4()),'w')
            if logList:
                print logList
                f.write(str(logList))
            else:
                print "empty list"     
            logList = []
            timeout = False
            f.close()
        except:
            print "File create error in timeout"
    timeout = True        
                    

application = tornado.web.Application([(r".*", MainHandler),])
logList = []
logFolder = './LogFolder'
timeout = False
#timeoutFunction()

if not os.path.exists(logFolder):
    try:
        os.makedirs(logFolder)
    except:
        print "Cannot create LogFolder"    

if __name__ == "__main__":
    tornado.options.parse_command_line()
    application.listen(9000)
    tornado.ioloop.PeriodicCallback(timeoutFunction, 60000).start()
    tornado.ioloop.IOLoop.instance().start()