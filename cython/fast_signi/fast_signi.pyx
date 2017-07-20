__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'


from libc.math cimport sqrt

cdef int _WINDOW_SIZE = 15

cdef double _ALPHA = 1.0 * _WINDOW_SIZE / (24*60)

cdef double _BETA = 1.0 * _WINDOW_SIZE

cdef int _ONE_MINUTE = 60


cdef class SignificanceScorer:

    cdef public double ewma
    cdef public double ewmvar
    cdef public double count
    cdef public double current_epoch
    cdef public double next_epoch

    @staticmethod
    def set_window_size(int _wz, int _cycle, double _average):
        global _WINDOW_SIZE
        global _ALPHA
        global _BETA

        _WINDOW_SIZE = _wz
        _ALPHA = 1.0 * _WINDOW_SIZE / _cycle
        _BETA = _average * _WINDOW_SIZE

        return _WINDOW_SIZE, _ALPHA, _BETA

    def __init__(self):
        self.ewma = 0.
        self.ewmvar = 0.

        self.count = 0.
        self.current_epoch = 0.
        self.next_epoch = 0.

    def observe(self, double _t, double _x):
        global _WINDOW_SIZE
        global _ALPHA
        global _BETA
        global _ONE_MINUTE

        cdef double d, estimated_sig
        cdef int step_over, step

        if self.current_epoch == 0.:
            self.current_epoch = _t
            self.next_epoch = self.current_epoch + _ONE_MINUTE * _WINDOW_SIZE

        if _t < self.next_epoch:
            self.count += _x
        else:
            d = self.count - self.ewma
            self.ewma = self.ewma + _ALPHA * d
            self.ewmvar = (1 - _ALPHA)*(self.ewmvar + _ALPHA * (d ** 2))

            step_over = int((_t - self.next_epoch) / (_ONE_MINUTE * _WINDOW_SIZE))

            for step in range(step_over):
                self.ewmvar = (1 - _ALPHA)*(self.ewmvar + _ALPHA * (self.ewma ** 2))
                self.ewma *= (1 - _ALPHA)

            self.current_epoch += (step_over + 1) * _ONE_MINUTE * _WINDOW_SIZE
            self.next_epoch += (step_over + 1) * _ONE_MINUTE * _WINDOW_SIZE

            self.count = _x

        estimated_sig = (self.count - max(self.ewma, _BETA)) / (sqrt(self.ewmvar) + _BETA)
        return self.count, self.ewma, self.ewmvar, estimated_sig
