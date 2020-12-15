from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")  # Identify all the words in each line of text

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield  (word, sum(counts))

# Brief Refresher: Python Generators

def generator():
    for i in range(3):
        yield i
print(generator())  # <generator object generator at 0x7fa222ce64d0>

for i in generator():
    print(i)

l = [0, 1, 2]
print(l == [i for i in generator()])
sum([0, 1, 2]) == sum(generator())