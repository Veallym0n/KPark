from tornado import tcpclient, gen, ioloop
import traceback
import rdd
import util

from collections import deque
from pdb import set_trace as st

class TCP(object):

    def __init__(self,host,port):
        self.host = host
        self.port = port
        self.client = tcpclient.TCPClient()
        self.stream = None
        self.task_queue = []
        self.result_queue = []
        self.compute_queue()
        self.report_queue()

    @gen.coroutine
    def connect(self):
        try:
            self.stream = yield self.client.connect(self.host,self.port)
        except Exception,e:
            print "WARNING: Could not connect Master ..."
            ioloop.IOLoop.instance().call_later(1,self.connect)
        else:
            self.stream.set_close_callback(self.on_close)
            self.register()
            self.read()

    def on_close(self):
        print 'WARNING: Master: Closed'
        if self.stream.closed():
            ioloop.IOLoop.instance().call_later(1,self.connect)



    @gen.coroutine
    def read(self):
        try:
            if self.stream:
                data = yield self.stream.read_until('\r\n')
                data = data[:-2]
                self.task_queue.append(data)
        except Exception,e:
            self.stream.close()
            
        ioloop.IOLoop.instance().add_timeout(0,self.read)


    def rdd_compute(self, data, dependencies):
        rd = rdd.RDD(data)
        rd.dependencies = dependencies
        return rd.compute()


    def compute_queue(self):
        if self.task_queue:
            try:
                taskid, data ,dependencies = util.unserial(self.task_queue.pop(0))
                result = (taskid, self.rdd_compute(data, dependencies))
                self.result_queue.append(result)
            except Exception,e:
                print 'Compute Err',e
                traceback.print_exc()
        ioloop.IOLoop.instance().add_timeout(0,self.compute_queue)


    def report_queue(self):
        if self.result_queue:
            result = self.result_queue.pop(0)
            self.write(util.serial(result))
        ioloop.IOLoop.instance().add_timeout(0,self.report_queue)


    @gen.coroutine
    def register(self):
        try:
            yield self.write(util.serial({"Register":True}))
        except Exception,e:
            print 'register',e

    @gen.coroutine
    def write(self,data):
        try:
            yield self.stream.write(data+'\r\n')
        except Exception,e:
            print 'write',e




def init():
    client = TCP('127.0.0.1',8141)
    client.connect()

if __name__=='__main__':
    init()
