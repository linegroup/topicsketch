__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'


import topicsketch.stemmer as stemmer
import fast_signi
import fast_smoother


import exp_config


_SIGNI_THRESHOLD = eval(exp_config.get('detection', 'detection_threshold'))
_SIGNI_TYPE = exp_config.get('detection', 'detection_signi_type')


class SparseSmootherContainer:
    _THRESHOLD_FOR_CLEANING = eval(exp_config.get('detection', 'threshold_for_cleaning'))
    _CAPACITY_FOR_CLEANING = eval(exp_config.get('detection', 'capacity_for_cleaning'))

    def __init__(self):
        self.container = {}

    def _clean(self, _timestamp):
        to_be_cleaned_up = []
        max_v = 0.
        for key, value in self.container.iteritems():
            value.observe(_timestamp, 0.)

            if _SIGNI_TYPE == 's':
                if value.ewma <= self._THRESHOLD_FOR_CLEANING:
                    to_be_cleaned_up.append(key)
                if value.ewma > max_v:
                    max_v = value.ewma

            if _SIGNI_TYPE == 'a':
                if value.v1 <= self._THRESHOLD_FOR_CLEANING:
                    to_be_cleaned_up.append(key)
                if value.v1 > max_v:
                    max_v = value.v1

        print 'cleaning', len(to_be_cleaned_up), 'items...', 'max_v', max_v
        for key in to_be_cleaned_up:
            self.container.pop(key)

    def get(self, _id, _timestamp):
        # check for cleaning
        if len(self.container) > self._CAPACITY_FOR_CLEANING:
            self._clean(_timestamp)

        # return
        if _id in self.container:
            return self.container[_id]
        else:
            if _SIGNI_TYPE == 's':
                sig_scorer = fast_signi.SignificanceScorer()
            if _SIGNI_TYPE == 'a':
                sig_scorer = fast_smoother.EWMASmoother()

            self.container[_id] = sig_scorer
            return sig_scorer


class SigProcessor:

    def __init__(self):
        self.sig_scorers = SparseSmootherContainer()
        self.timestamp = None

    def process(self, _ptweet):

        self.timestamp = _ptweet.timestamp
        tokens = _ptweet.tokens

        # stemming
        tokens = map(lambda x: stemmer.stem(x), tokens)

        if len(tokens) < 3:
            return None, None

        set_of_tokens = set()
        for token in tokens:
            set_of_tokens.add(token)

        max_sig = 0.
        max_sig_instance = None
        sig_list = list()
        for token1 in set_of_tokens:
            for token2 in set_of_tokens:
                if token1 >= token2:
                    continue
                token = token1 + ',' + token2
                if _SIGNI_TYPE == 's':

                    count, ewma, ewmavar, sig = self.sig_scorers.get(token, self.timestamp).observe(self.timestamp, 1.0)
                    if sig > max_sig:
                        max_sig = sig
                        max_sig_instance = _ptweet.datetime(), count, ewma, ewmavar, sig, token
                    if sig > _SIGNI_THRESHOLD:
                        sig_list.append((_ptweet.datetime(), count, ewma, ewmavar, sig, token))

                if _SIGNI_TYPE == 'a':

                    _, v, sig = self.sig_scorers.get(token, self.timestamp).observe(self.timestamp, 1.0)
                    if sig > max_sig:
                        max_sig = sig
                        max_sig_instance = _ptweet.datetime(), 0, 0, v, sig, token
                    if sig > _SIGNI_THRESHOLD:
                        sig_list.append((_ptweet.datetime(), 0, 0, v, sig, token))

        if max_sig > _SIGNI_THRESHOLD:
            return max_sig_instance, sig_list

        return None, None


















