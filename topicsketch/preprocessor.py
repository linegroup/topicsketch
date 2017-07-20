__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'

from string import punctuation
import re

import twokenize

import stop_words
import stream

_PUN_PATTERN = re.compile('^[' + punctuation + ']+$')


class Preprocessor(stream.ItemStream):

    def __init__(self, _tw_stream):
        self.tw_stream = _tw_stream

    def next(self):
        tweet = self.tw_stream.next()

        if tweet is stream.End_Of_Stream:
            return stream.End_Of_Stream

        if tweet is None:
            return None

        t = tweet.timestamp
        uid = tweet.uid
        txt = tweet.str

        # lower case
        txt = txt.lower()

        # tokenize
        try:
            tokens = twokenize.tokenizeRawTweetText(txt)
        except:
            return None

        # filter
        tokens = filter(lambda x: (not stop_words.contains(x)) and (not _PUN_PATTERN.match(x)) and (len(x) <= 32) and (len(x) > 1), tokens)

        return stream.PreprocessedTweetItem(t, uid, tokens)
