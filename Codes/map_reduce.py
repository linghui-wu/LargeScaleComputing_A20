# Word count pseudocode

def mapper(line):
    for word in line.split():
        output(word, 1)

def combiner(key, values):
    output(key, sum(values))

def reducer(key, values):
    output(key, sum(values))

