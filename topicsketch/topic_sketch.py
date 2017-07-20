__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'


import math
from collections import deque
from datetime import datetime

from scipy.sparse import dok_matrix
from scipy.sparse import csr_matrix
import numpy as np
import json

import fast_hashing
import fast_smoother
import solver
import stemmer
import postprocessor

import experiment.exp_config as config


_SKETCH_BUCKET_SIZE = eval(config.get('sketch', 'sketch_bucket_size'))

_NUM_TOPICS = eval(config.get('sketch', 'num_topics'))

_PROBABILITY_THRESHOLD = eval(config.get('sketch', 'probability_threshold'))
if _PROBABILITY_THRESHOLD == 0.:
    _PROBABILITY_THRESHOLD = 1./_SKETCH_BUCKET_SIZE

_ACTIVE_WINDOW_SIZE = eval(config.get('sketch', 'active_window_size'))


class SparseSmootherContainer():
    _THRESHOLD_FOR_CLEANING = eval(config.get('sketch', 'threshold_for_cleaning'))
    _CAPACITY_FOR_CLEANING = eval(config.get('sketch', 'capacity_for_cleaning'))

    def __init__(self):
        self.container = {}

    def close(self):
        pass

    def _clean(self, _timestamp):
        to_be_cleaned_up = []
        for key, value in self.container.iteritems():
            tp = value.get(_timestamp)
            if not tp:
                print _timestamp, value.timestamp
                print 'stream item seems out of time order!'
                continue
            t ,v ,a = tp
            if v <= self._THRESHOLD_FOR_CLEANING: # check v
                to_be_cleaned_up.append(key)

        print 'cleaning', len(to_be_cleaned_up), 'items...'
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

            _smoother = fast_smoother.EWMASmoother()

            self.container[_id] = _smoother
            return _smoother


class TopicSketch:

    def __init__(self):
        self.output = open('./results.txt', 'w', 0)

        self.sketch_m2 = [SparseSmootherContainer() for i in range(fast_hashing.HASH_NUMBER)]
        self.sketch_m3 = [SparseSmootherContainer() for i in range(fast_hashing.HASH_NUMBER)]

        self.timestamp = 0

        self.active_terms = deque([])

        np.random.seed(0)
        self.random_projector = [np.random.rand(_SKETCH_BUCKET_SIZE) for i in xrange(fast_hashing.HASH_NUMBER)]

        self.alpha2 = 1e-1
        self.alpha3 = 1e-2

    def close(self):
        self.output.close()

    @staticmethod
    def _index(i, j):
        return i * _SKETCH_BUCKET_SIZE + j

    @staticmethod
    def _inverse_index(_id):
        i = _id / _SKETCH_BUCKET_SIZE
        _id -= i * _SKETCH_BUCKET_SIZE

        j = _id

        return i, j

    @staticmethod
    def laplace_smooth(x1, x2, n, alpha=1.):
        return (x1 + alpha) / (x2 + n * alpha)

    def pre_process(self, uid, tokens):
        # adding into active terms before stemming
        self.active_terms.append((self.timestamp, tokens, uid))

        while len(self.active_terms) > 0:
            term = self.active_terms[0]
            if term[0] < self.timestamp - _ACTIVE_WINDOW_SIZE * 60:
                self.active_terms.popleft()
            else:
                break

        # stemming
        tokens = map(lambda x: stemmer.stem(x), tokens)

        if len(tokens) < 1:
            return None

        # hashing
        results = [] # (counts, reserved_slot, n_words, h)

        for h in range(fast_hashing.HASH_NUMBER):
            results.append(({}, {}, len(tokens), h))

        for token in tokens:
            hash_code = np.array(fast_hashing.hash_code(token)) % _SKETCH_BUCKET_SIZE

            for h in range(fast_hashing.HASH_NUMBER):
                code = hash_code[h]
                if code in results[h][0]:
                    results[h][0][code] += 1
                else:
                    results[h][0][code] = 1

        return results

    def process_m2_unit(self, _count, _n_words, _h):
        s = _n_words * (_n_words - 1.0)

        n_distinct_words = len(_count)

        for k1 in _count:
            for k2 in _count:
                if k1 > k2:
                    continue
                if k1 == k2:
                    num = _count[k1] * (_count[k1] - 1)
                else:
                    num = _count[k1] * _count[k2]

                if num == 0:
                    continue

                num = self.laplace_smooth(num, s, n_distinct_words ** 2, self.alpha2)

                self.sketch_m2[_h].get(self._index(k1, k2), self.timestamp).observe(self.timestamp, num)

    def process_m3_unit(self, _count, _n_words, _h):
        s = _n_words * (_n_words - 1.0) * (_n_words - 2.0)

        n_distinct_words = len(_count)
        for k1 in _count:
            for k2 in _count:
                if k1 > k2:
                    continue
                num = 0
                for k3 in _count:

                    if k1 == k2 and k2 == k3:
                        v = _count[k1] * (_count[k1] - 1) * (_count[k1] - 2)
                    elif k1 == k2:
                        v = _count[k1] * (_count[k1] - 1) * _count[k3]
                    elif k2 == k3:
                        v = _count[k2] * (_count[k2] - 1) * _count[k1]
                    elif k3 == k1:
                        v = _count[k3] * (_count[k3] - 1) * _count[k2]
                    else:
                        v = _count[k1] * _count[k2] * _count[k3]

                    v = self.laplace_smooth(v, s, n_distinct_words ** 3, self.alpha3)
                    num += v * self.random_projector[_h][k3]

                if num == 0:
                    continue

                self.sketch_m3[_h].get(self._index(k1, k2), self.timestamp).observe(self.timestamp, num)

    def process_unit(self, _results):
        _count = _results[0]
        _n_words = _results[2]
        _h = _results[3]

        self.process_m2_unit(_count, _n_words, _h)
        self.process_m3_unit(_count, _n_words, _h)

    def process(self, _ptweet):
        self.timestamp = _ptweet.timestamp

        results = self.pre_process(_ptweet.uid, _ptweet.tokens)

        if results is None:
            return

        map(self.process_unit, results)

    def infer_unit(self, _h):
        k = _NUM_TOPICS
        n = _SKETCH_BUCKET_SIZE

        m2 = dok_matrix((n, n), dtype=np.float64)
        m3 = dok_matrix((n, n), dtype=np.float64)

        container = self.sketch_m2[_h]
        for key, value in container.container.iteritems():
            i, j = self._inverse_index(key)

            m2[i,j] = value.get(self.timestamp)[2]
            if i != j:
                m2[j,i] = m2[i,j]

        container = self.sketch_m3[_h]
        for key, value in container.container.iteritems():
            i, j = self._inverse_index(key)

            m3[i,j] = value.get(self.timestamp)[2]
            if i != j:
                m3[j,i] = m3[i,j]

        return solver.solve(csr_matrix(m2), csr_matrix(m3), n, k)

    @staticmethod
    def refine_prob(_prob):
        prob = _prob.real
        prob = map(lambda x: x if x > 0 else 0, prob)
        prob = prob / sum(prob)
        return prob

    def run_time_infer(self):
        infer_results = map(self.infer_unit, range(fast_hashing.HASH_NUMBER))

        probs = list()
        for i in xrange(len(infer_results)):
            a = infer_results[i][0]
            a = a.real.tolist()
            k = a.index(max(a))
            print 'a=', a[k]
            v = infer_results[i][2]
            prob = self.refine_prob(v[:, k])
            probs.append(prob)

        self.analyse_topics(probs)

    def analyse_topics(self, _probs):
        words = set()
        for term in self.active_terms:
            for word in term[1]:
                words.add(word)
        print "size of words:", len(words)

        high_prob_words = []
        for _word in words:
            word = stemmer.stem(_word)
            hash_code = np.array(fast_hashing.hash_code(word)) % _SKETCH_BUCKET_SIZE
            min_prob_list = []
            for h in range(fast_hashing.HASH_NUMBER):
                prob = _probs[h][hash_code[h]]
                min_prob_list.append(prob)

            min_prob_list.sort()
            min_prob = min_prob_list[1] # !!!
            if min_prob >= _PROBABILITY_THRESHOLD:
                high_prob_words.append((word, min_prob))

        # rescale
        s_prob = sum([p for w, p in high_prob_words])
        high_prob_words = [(w, p/s_prob) for w, p in high_prob_words]

        high_prob_words.sort(key=lambda x: x[1], reverse=True)

        # top 20
        high_prob_words = high_prob_words[:20]

        post_res = postprocessor.process(high_prob_words, self.active_terms)

        if eval(config.get('output', 'debug_info')):
            self.output.write('high_prob_words\n')
            self.output.write(high_prob_words) #debugging
            self.output.write('\npost_res\n')
            self.output.write(post_res) #debugging
            self.output.write('\n')

        flag, word_level_results, _ = post_res
        if flag:
            event = dict()
            event['detection_time'] = datetime.utcfromtimestamp(self.timestamp)
            event_words = list()
            for prob_word, word_flag in zip(high_prob_words, word_level_results):
                _word = prob_word[0]
                if word_flag:
                    event_words.append(_word)

            event['key_words'] = event_words

            self.output.write(json.dumps(event))
            self.output.write('\n')










