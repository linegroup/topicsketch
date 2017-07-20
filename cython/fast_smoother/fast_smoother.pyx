__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'



from libc.math cimport exp



cdef int _ONE_UNIT = 60 # seconds


cdef double _WINDOW_SIZE = 15.

cdef double _WINDOW_SIZE1 = 15.

cdef double _WINDOW_SIZE2 = 16.


def set_unit_size(int _uz):
    global _ONE_UNIT

    _ONE_UNIT = _uz
    return _ONE_UNIT


cdef class EWMASmoother: # Exponentially Weighted Moving Average Smoother

    cdef public double v1, v2
    cdef public double timestamp

    @staticmethod
    def set_window_size(double _wz1, double _wz2):
        global _WINDOW_SIZE1
        global _WINDOW_SIZE2

        _WINDOW_SIZE1 = _wz1
        _WINDOW_SIZE2 = _wz2

        return _WINDOW_SIZE1, _WINDOW_SIZE2

    def __init__(self):
        self.v1 = 0
        self.v2 = 0
        self.timestamp = 0

    def observe(self, double _timestamp, double _number): # observe a new incoming item, and return current status
        global _WINDOW_SIZE1
        global _WINDOW_SIZE2

        cdef double e1, e2, dt

        if self.timestamp != 0:
            dt = (self.timestamp - _timestamp) / _ONE_UNIT

            if dt != 0:
                e1 = exp(dt/_WINDOW_SIZE1)
                e2 = exp(dt/_WINDOW_SIZE2)
                self.v1 *= e1
                self.v2 *= e2

        self.timestamp = _timestamp

        self.v1 += _number/_WINDOW_SIZE1
        self.v2 += _number/_WINDOW_SIZE2

        return _timestamp, self.v1, (self.v1 - self.v2)/(_WINDOW_SIZE2 - _WINDOW_SIZE1)


    def get(self, double _timestamp): # return current status
        global _WINDOW_SIZE1
        global _WINDOW_SIZE2

        cdef double e1, e2, dt

        if _timestamp < self.timestamp:
            return None

        dt = (self.timestamp - _timestamp) / _ONE_UNIT

        if dt == 0:
            return _timestamp, self.v1, (self.v1 - self.v2)/(_WINDOW_SIZE2 - _WINDOW_SIZE1)

        e1 = exp(dt/_WINDOW_SIZE1)
        e2 = exp(dt/_WINDOW_SIZE2)

        return _timestamp, e1 * self.v1, (e1 * self.v1 - e2 * self.v2)/(_WINDOW_SIZE2 - _WINDOW_SIZE1)