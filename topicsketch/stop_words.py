__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'

_stop_words = set([line.strip() for line in open('./twitter-stopwords.txt')])


def contains(word):
    return word in _stop_words
