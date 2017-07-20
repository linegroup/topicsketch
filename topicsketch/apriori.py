__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'



class Transaction:
    # each transaction contains a set of words, a group id and an in-group index

    def __init__(self, _words, _group_id, _index):
        self.words = _words
        self.group_id = _group_id
        self.index = _index

class Candidate:
    # each candidate contains a set of words and a supporting record

    def __init__(self, _words):
        self.words = _words
        self.support = {}

    def be_supported_by(self, _group_id, _index):
        self.support[_group_id] = _index

    def supporting_value(self):
        return len(self.support)


class SearchableSetList:
    #  a list in which a set can be efficiently found

    def __init__(self):
        self.list = {} # the list is a hash table

    @staticmethod
    def _hash(s):  # return the hash code of a set of words
        h = 0
        for w in s:
            h += hash(w)
        return h

    def append(self, s):  # s is a set of words
        h = self._hash(s)

        if h in self.list:
            self.list[h].append(s)
        else:
            self.list[h] = [s]

    def contains(self, s):  # s is a set of words
        h = self._hash(s)

        if h not in self.list:
            return False

        return s in self.list[h]

    def full_list(self):
        ret = []
        for l in self.list.values():
            ret += l
        return ret


def iteration(transactions, mini_support, L1):
    # from L(k) to C(k+1)
    C2 = []
    candi_words = SearchableSetList()
    for i in xrange(len(L1)):
        for j in xrange(i):
            s = L1[i].words.union(L1[j].words)
            if not candi_words.contains(s):
                candi_words.append(s)

    for words in candi_words.full_list():
        C2.append(Candidate(words))

    # from C(k+1) to L(k+1)
    L2 = []
    for transaction in transactions:
        for candidate in C2:
            if transaction.words.issuperset(candidate.words):
                candidate.be_supported_by(transaction.group_id, transaction.index)

    for candidate in C2:
        if candidate.supporting_value() >= mini_support:
            L2.append(candidate)

    return L2


def reduce(L):
    output = []
    for l in L[::-1]:
        words = l.words
        flag = True
        for s in output:
            if words.issubset(s.words):
                flag = False
                break
        if flag:
            output.append(l)

    return output


def apriori(transactions, mini_support):

    output = []

    # generate L1
    L1 = []
    words = {}

    for transaction in transactions:
        for word in transaction.words:
            if word not in words:
                words[word] = Candidate(set([word]))

            words[word].be_supported_by(transaction.group_id, transaction.index)

    for word in words:
        candidate = words[word]
        if candidate.supporting_value() >= mini_support :
            L1.append(candidate)

    # start iterations
    while len(L1) > 0:
        output += L1
        L2 = iteration(transactions, mini_support, L1)
        L1 = L2

    return reduce(output)


def test():
    transactions = list()
    transactions.append(Transaction(set(['a', 'b']), 1, 1))
    transactions.append(Transaction(set(['a', 'c']), 1, 2))
    transactions.append(Transaction(set(['a', 'd']), 1, 3))
    transactions.append(Transaction(set(['a', 'b']), 2, 1))
    transactions.append(Transaction(set(['a', 'e']), 2, 2))
    transactions.append(Transaction(set(['b', 'c']), 2, 3))
    transactions.append(Transaction(set(['a', 'b']), 3, 1))
    transactions.append(Transaction(set(['b', 'c']), 3, 2))

    for candi in apriori(transactions, 2):
        print candi.words, candi.support
    print '-----------------------'
    for candi in apriori(transactions, 3):
        print candi.words, candi.support