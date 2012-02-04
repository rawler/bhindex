

class Counter(object):
    def __init__(self):
        self.i = 0

    def inc(self, i = 1):
        res = self.i
        self.i += i
        return res

    def __int__(self):
        return self.i

    def inGibi(self):
        return self.i / (1024*1024*1024.0)


