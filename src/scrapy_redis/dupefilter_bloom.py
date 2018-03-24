import redis
import hashlib
from scrapy.utils.request import request_fingerprint

import logging
import time

from . import defaults
from .connection import get_redis_from_settings

logger = logging.getLogger(__name__)

class SimpleHash(object):
    '''
    Calculate the location of the map to bitmap through the algorithm
    '''
    def __init__(self, cap, seed):
        self.cap = cap
        self.seed = seed

    def hash(self, value):
        '''

        :param value:
        :return:
        '''
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.cap - 1) & ret


class BloomDupeFilter(object):
   # def __init__(self, host='localhost', port=6379, db=0, blockNum=3, key='bloomfilter'):

    logger = logger

    def __init__(self , server, db, blockNum, key ,debug ):
        """
        :param host: the host of Redis
        :param port: the port of Redis
        :param db: witch db in Redis
        :param blockNum: one blockNum for about 90,000,000; if you have more strings for filtering, increase it.
        :param key: the key's name in Redis

        <bit_size>
        The maximum capacity of Redis's String type is 512M. When key value is too large,
        it will affect the query efficiency of redis. Therefore, using 256M,
        when the false positive rate =8.65e-5 is 1/11700, the number of duplicated strings can be 93 million.
        refer to this table http://img.blog.csdn.net/20161110104702907

        <key>
        The K value, which ultimately represents the number of hash functions
        that participate in the calculation of the "bit", and the lower the miscarriage rate.

        <blockNum>
        When blocknum is equal to 1, bloomfilter will apply for 256M memory
        from the redis server.
        """
        self.server = server
        self.bit_size = 1 << 31   #256*10^20*8 bit
        self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.key = key
        self.blockNum = blockNum
        self.hashfunc = []
        self.db = db
        self.debug = debug
        self.logdupes = True

        for seed in self.seeds:
            self.hashfunc.append(SimpleHash(self.bit_size, seed))

    def request_seen(self, request):
        '''Determine whether requests exists,return true if exists'''

        if not request:
           return False
        encrypted_request = self.request_to_encrypt(request)
        ret = True
        name = self.key + str(int(encrypted_request[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
           loc = f.hash(encrypted_request)
           ret = ret & self.server.getbit(name, loc)
        return ret

    def close(self, reason=''):
        """Delete data on close. Called by scheduler_bloom from Scrapy-redis.

        Parameters
        ----------
        reason : str, optional

        """
        self.clear()
    def clear(self):
        """Clears bitmap(str) key from redis."""
        self.server.delete(self.key)

    def request_to_encrypt(self,request):
        '''Encrypts Request then return '''
        fp = request_fingerprint(request)

        sha1 = hashlib.sha1()
        sha1.update(fp)
        xxoo = sha1.hexdigest()

        return xxoo

    def insert(self, request):
        '''Inserts encrypted Request to Redis's bitmap'''
        encrypted_request = self.request_to_encrypt(request)
        name = self.key + str(int(encrypted_request[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
            loc = f.hash(encrypted_request)
            self.server.setbit(name, loc, 1)


    def log(self, request, spider):
        """Logs given request.

        Parameters
        ----------
        request : scrapy.http.Request
        spider : scrapy.spiders.Spider

        """
        if self.debug:
            msg = "(Bloom)Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("(Bloom)Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False