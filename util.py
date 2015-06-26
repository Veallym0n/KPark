import cloudpickle
import cPickle
import zlib

def serial(obj):
    return zlib.compress(cloudpickle.dumps(obj)).encode('base64')


def unserial(data):
    return cPickle.loads(zlib.decompress(data.decode('base64')))


