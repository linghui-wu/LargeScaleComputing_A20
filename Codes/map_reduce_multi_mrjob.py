from mrjob.job import MRJob 
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")  # Identify all the words in each line of text

class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                    combiner=self.combiner_count_words,
                    reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word) 

    def reducer_find_max_word(self, _, word_count_pairs):
        # So yielding one results in key=counts, value=word
        yield max(word_count_pairs)

if __name__ == '__main__':
    # MRMostUsedWord.run()

    # max() identifies the maximum *first* value in a tuple
    word_counts = [("cat", 1), ("bat", 2)]
    print(max(word_counts))

    # This time, let's get the word that occurs most frequently
    word_counts = [(1, "cat"), (2, "bat")]
    print(max(word_counts))
    
