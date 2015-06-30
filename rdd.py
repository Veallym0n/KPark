#Resilient Distributed Datasets
from uuid import uuid4
from itertools import chain
from collections import defaultdict

class RDD:

    def __repr__(self):
        return '<RDD %d>' % self.size

    def __init__(self,datas=None,numSplits=2,ctx=None):
        self.ctx = ctx
        if self.ctx:
            self.taskid = uuid4().hex
        else:
            self.taskid = None
        self.size = len(datas)
        self.datasets = datas
        self.dependencies = []
        if not self.ctx:
            self.numSplits = numSplits
        else:
            if self.ctx.NodeList:
                self.numSplits = len(self.ctx.NodeList)
            else:
                print 'has no compute client connected, change to local'
                self.ctx = None
        self.splits = []
        self.splitsRes = []

    def __len__(self):
        return len(self.splits)


    def _splits(self):
        pieceNum = len(self.datasets)/self.numSplits
        appendNum = int(bool(len(self.datasets) % self.numSplits))
        self.splits = [self.datasets.__getslice__(i*pieceNum,i*pieceNum+pieceNum) for i in xrange(self.numSplits+appendNum)]
        return self

    # ------------- Transform ------------

    def map(self,func,func_name=None):
        self.dependencies.append(lambda d:map(func,d))
        return self

    def filter(self,func,func_name=None):
        self.dependencies.append(lambda d:filter(func,d))
        return self

    def use(self,func,func_name=None):
        self.dependencies.append(lambda d:func(d))
        return self

    # ------------- Collect ---------------

    def collect(self):
        if not self.ctx:
            return self.compute()
        elif not self.dependencies:
            return self.compute()
        else:
            return self.distribute_compute()

    def collectAsMap(self):
        return dict(self.collect())


    # ------------- Action ---------------


    def cache(self):
        rddn = RDD(self.collect(),ctx=self.ctx)
        rddn.dependencies = self.dependencies
        return rddn

    def groupby(self,keyfunc):
        d = defaultdict(list)
        for items in self.collect():
            key = keyfunc(items)
            d[key].append(items)
        return RDD(d.items(),ctx=self.ctx)

    def groupByKey(self):
        d = defaultdict(list)
        for key,value in self.collect():
            d[key].append(value)
        return RDD(d.items(),ctx=self.ctx)

    def groupByValue(self):
        d = defaultdict(list)
        for key,value in self.collect():
            d[value].append(k)
        return RDD(d.items(),ctx=self.ctx)

    def sort(self,key=None,reverse=False):
        data = self.collect()
        data.sort(key=key,reverse=reverse)
        return RDD(data,ctx=self.ctx)

    def dist_sort(self,key=None,reverse=False):
        return RDD(self.collect(),ctx=self.ctx).use(lambda x:sorted(x,key=key,reverse=reverse)).sort(key=key,reverse=reverse)

    def reduce(self,func):
        return RDD(reduce(func,self.collect()),ctx=self.ctx)

    def reduceByKey(self,func):
        return self.groupByKey().map(lambda x:(x[0],reduce(func,x[1])))

    def uniq(self):
        return RDD(list(set(self.collect())),ctx=self.ctx)

    def top(self, n=10, key=None, reverse=False):
        if self.ctx:
            return RDD(self.collect(),ctx=self.ctx).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).dist_sort(key=key,reverse=reverse).collect()[:n]
        else:
            return RDD(self.collect(),ctx=self.ctx).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sort(key=key,reverse=reverse).collect()[:n]

    def hot(self, n=10):
        return self.top(n,key=lambda x:x[1],reverse=True)
    
    def count(self):
        return len(self.collect())

    def take(self,n=1):
        ''' oh my god. i could not easily implement take function!! '''
        return self.collect()[:n]

    def first(self):
        r = self.take(1)
        if r: return r[0]

    # ------------- worker ------------------

    def compute(self):
        if not self.dependencies: return self.datasets
        r = self.datasets
        for func in self.dependencies:
            r = func(r)
        return list(r)


    def distribute_compute(self):
        self._splits()
        self.ctx.create_task(self.taskid,self)
        self.ctx.submit_task(self.taskid,self.splits)
        while True:
            if len(self.splitsRes)>=len(self.splits):
                break
        result = list(chain.from_iterable(self.splitsRes))
        del self.ctx.tasks[self.taskid]
        return result

