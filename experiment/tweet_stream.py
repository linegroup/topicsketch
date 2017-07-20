__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'

from datetime import date, timedelta as td

import time

import MySQLdb

import topicsketch.stream as stream

import exp_config

import sys


class TweetStreamFromDB(stream.ItemStream):

    def __init__(self):
        _start_y = int(exp_config.get('stream', 'start_y'))
        _start_m = int(exp_config.get('stream', 'start_m'))
        _start_d = int(exp_config.get('stream', 'start_d'))

        _end_y = int(exp_config.get('stream', 'end_y'))
        _end_m = int(exp_config.get('stream', 'end_m'))
        _end_d = int(exp_config.get('stream', 'end_d'))

        self.table = exp_config.get('stream', 'table')

        self.dy_start = date(_start_y, _start_m, _start_d)
        self.dy_end = date(_end_y,_end_m,_end_d)

        self.host = exp_config.get('stream', 'host')
        self.user = exp_config.get('stream', 'user')
        self.passwd = exp_config.get('stream', 'passwd')
        self.db = exp_config.get('stream', 'db')

        self.connection = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')

        self.cursor = self.connection.cursor()

        _time0 = self.dy_start.strftime("%Y-%m-%d")
        _time1 = (self.dy_start + td(days=1)).strftime("%Y-%m-%d")

        sql_str = 'select * from ' + self.table + ' where  t >= "%s" and t < "%s" order by t' % (_time0, _time1)
        print sql_str

        self.cursor.execute(sql_str)

    def next(self):
        row = self.cursor.fetchone()

        if row is None:
            try:
                self.cursor.close()
                self.connection.close()
            except:
                print 'closing error...'

            self.dy_start = self.dy_start + td(days=1)
            if self.dy_start < self.dy_end:
                flag = True
                while flag:
                    try:
                        self.connection = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')

                        self.cursor = self.connection.cursor()

                        _time0 = self.dy_start.strftime("%Y-%m-%d")
                        _time1 = (self.dy_start + td(days=1)).strftime("%Y-%m-%d")

                        sql_str = 'select * from ' + self.table + ' where  t >= "%s" and t < "%s" order by t' % (_time0, _time1)
                        print sql_str

                        self.cursor.execute(sql_str)

                        row = self.cursor.fetchone()

                        flag = False
                    except:
                        print 'building connection fail...'
                        print sys.exc_info()
                        time.sleep(60)
                        print 'rebuild connection...'
            else:
                return stream.End_Of_Stream

        if row is None:
            return None

        _t = row[0]
        _user = row[2]
        _tweet = row[3]
        item = stream.RawTweetItem(_t, _user, _tweet)
        return item


