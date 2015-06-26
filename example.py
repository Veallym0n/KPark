import rdd
import Server

Server.initServer()
Server.initIO()


def example():
    import random

    data = [random.randint(66,76) for i in xrange(100)]
    result = rdd.RDD(data,ctx=Server.Context).map(chr).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).collectAsMap()
    print result


if __name__=='__main__':
    example()
    while True:
        pass
