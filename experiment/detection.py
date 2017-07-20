__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'


import datetime

import signi_processor

import fast_signi

import fast_smoother

import topicsketch.stream as ts_stream

import exp_config


_THREAD_GAP = eval(exp_config.get('detection', 'thread_gap'))


class Slice:

    def __init__(self):
        self.start = 0.0
        self.end = 0.0
        self.keywords = None
        self.sig = 0.0
        self.first_sig = 0.0
        self.thread = []
        self.first_keywords = None

    def new_thread(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance
        self.start = _t
        self.end = _t
        self.keywords = set(_keywords.split(','))
        self.thread.append(sig_instance)
        self.first_sig = _sig
        self.sig = _sig
        self.first_keywords = _keywords

    def add_to_thread(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance

        kw1, kw2 = _keywords.split(',')

        if len(self.keywords) >= 10:
            return False

        if _t - self.end <= datetime.timedelta(minutes=1):
            if kw1 not in self.keywords and kw2 not in self.keywords:
                return False
        elif kw1 not in self.keywords or kw2 not in self.keywords:
            return False

        if _t - self.end > datetime.timedelta(minutes=_THREAD_GAP):
            return False

        if _sig > self.sig:
            self.sig = _sig

        self.end = _t
        self.thread.append(sig_instance)
        self.keywords.add(kw1)
        self.keywords.add(kw2)

        return True


class DetectionComponent(ts_stream.ItemStream):

    def __init__(self, _ptw_stream):
        self.ptw_stream = _ptw_stream

        _wz = eval(exp_config.get('significance', 'window_size'))
        _cycle = eval(exp_config.get('significance', 'cycle'))
        _average = eval(exp_config.get('significance', 'average'))
        print 'significance # set parameters:' + str(fast_signi.SignificanceScorer.set_window_size(_wz, _cycle, _average))

        _uz = eval(exp_config.get('acceleration', 'unit_size'))
        print 'acceleration # set unit size ' + str(fast_smoother.set_unit_size(_uz))

        _wz1 = eval(exp_config.get('acceleration', 'window_size1'))
        _wz2 = eval(exp_config.get('acceleration', 'window_size2'))
        print 'acceleration # set windows ' + str(fast_smoother.EWMASmoother.set_window_size(_wz1, _wz2))

        self.processor = signi_processor.SigProcessor()

        self.threads = list()

        _start_t = exp_config.get('detection', 'start_t')
        _end_t = exp_config.get('detection', 'end_t')

        self._start_t = datetime.datetime.strptime(_start_t, '%Y-%m-%d %H:%M:%S')
        self._end_t = datetime.datetime.strptime(_end_t, '%Y-%m-%d %H:%M:%S')

    def process(self, sig_instance, sig_list=None):

        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance

        if _t < self._start_t or _t > self._end_t:
            return 0.

        if eval(exp_config.get('output', 'debug_info')):
            if sig_list:  # for debugging
                print '-----------------------'
                for sig_ in sig_list:
                    print '__sig__', sig_
                print '-----------------------'

        create_new = True

        for thread in self.threads:
            if thread.add_to_thread(sig_instance):
                create_new = False
                break

        if create_new:
            thread = Slice()
            thread.new_thread(sig_instance)

            self.threads.append(thread)

            return _sig

        return 0.

    def next(self):
        ptweet = self.ptw_stream.next()

        if ptweet is ts_stream.End_Of_Stream:
            return ts_stream.End_Of_Stream

        if ptweet is None:
            return None

        sig_instance, sig_list = self.processor.process(ptweet)

        if sig_instance is not None:
            output = self.process(sig_instance, sig_list)

            if eval(exp_config.get('output', 'debug_info')):
                print sig_instance
            return ptweet, output

        return ptweet, 0.0







