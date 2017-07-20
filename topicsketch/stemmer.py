__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'

from nltk.stem.lancaster import *
from nltk.stem.snowball import *
from nltk.stem.porter import *

import experiment.exp_config as config

stemmer = None
if config.get('pre_process', 'stemmer') == 'Snowball':
    stemmer = SnowballStemmer("english")

if config.get('pre_process', 'stemmer') == 'Porter':
    stemmer = PorterStemmer()

if config.get('pre_process', 'stemmer') == 'Lancaster':
    stemmer = LancasterStemmer()


def stem(word):
    if stemmer is None:
        return word

    try:
        ret = stemmer.stem(word)
        ret = str(ret)
    except:
        ret = word

    return ret