__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'



import calendar
import datetime


class NumberItem:

    def __init__(self, _t, _number):
        if isinstance(_t, datetime.datetime):
            self.timestamp = calendar.timegm(_t.timetuple())
        else:
            self.timestamp = _t

        self.number = _number

    def datetime(self):
        return datetime.datetime.utcfromtimestamp(self.timestamp)


class StringItem:

    def __init__(self, _t, _str):
        if isinstance(_t, datetime.datetime):
            self.timestamp = calendar.timegm(_t.timetuple())
        else:
            self.timestamp = _t

        self.str = _str

    def datetime(self):
        return datetime.datetime.utcfromtimestamp(self.timestamp)


class RawTweetItem:

    def __init__(self, _t, _uid, _str):
        if isinstance(_t, datetime.datetime):
            self.timestamp = calendar.timegm(_t.timetuple())
        else:
            self.timestamp = _t

        self.str = _str
        self.uid = _uid

    def datetime(self):
        return datetime.datetime.utcfromtimestamp(self.timestamp)


class PreprocessedTweetItem:

    def __init__(self, _t, _uid, _tokens):
        if isinstance(_t, datetime.datetime):
            self.timestamp = calendar.timegm(_t.timetuple())
        else:
            self.timestamp = _t

        self.tokens = _tokens
        self.uid = _uid

    def datetime(self):
        return datetime.datetime.utcfromtimestamp(self.timestamp)


class EndOfStream:

    def __init__(self):
        pass


End_Of_Stream = EndOfStream()


class ItemStream:

    def next(self):
        pass


class ItemStreamFromList(ItemStream):

    def __init__(self, _list):
        self.index = -1;
        self.list = _list

    def next(self):
        self.index += 1
        if self.index < len(self.list) :
            return self.list[self.index]
        return None

    def reset(self):
        self.index = -1;