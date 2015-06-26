import tornado.ioloop
import tornado.iostream
from tornado import tcpserver,gen

import util
from itertools import cycle

Context = None

class masterServer(tcpserver.TCPServer):


    def __init__(self,ioloop=tornado.ioloop.IOLoop.instance()):
        tcpserver.TCPServer.__init__(self,io_loop=ioloop)
        self.tasks = {}
        self.NodeList = {}
        self.nodes = cycle(self.NodeList)


    def closeSocket(self, stream, address):
        stream.close()
        if address in self.NodeList:
            print address,'has been disconnected\n'
            del self.NodeList[address]
            self.nodes = cycle(self.NodeList)


    @gen.coroutine
    def handle_stream(self, stream, address):
        while True:
            try:
                if not stream.closed():
                    yield self.read(stream,address)
                else:
                    break
            except tornado.iostream.StreamClosedError:
                self.closeSocket(stream,address)

    @gen.coroutine
    def read(self,stream,addr):
        try:
            data = yield stream.read_until('\r\n')
            data = data[:-2]
            if util.unserial(data)=={"Register":True}:
                self.NodeList[addr]=stream
                self.nodes = cycle(self.NodeList)
                print 'new Node Comes',addr,'\n'
            else:
                taskid,data = util.unserial(data)
                self.tasks[taskid].splitsRes.append(data)
        except Exception,e:
            self.closeSocket(stream, addr)

    @gen.coroutine
    def submit_task(self,taskid, splits):
        dependencies = self.tasks[taskid].dependencies
        for data in splits:
            taskitem = taskid,data,dependencies
            stream = self.NodeList.get(self.nodes.next(),None)
            if stream:
                stream.socket.send(util.serial(taskitem)+'\r\n')

    def create_task(self,taskid, ins_rdd):
        self.tasks[taskid] = ins_rdd

def initIO():
    import threading
    t = threading.Thread(target=tornado.ioloop.IOLoop.instance().start)
    t.daemon = True
    t.start()


def initServer():
    global Context
    Context = masterServer()
    Context.listen(8141)
    
