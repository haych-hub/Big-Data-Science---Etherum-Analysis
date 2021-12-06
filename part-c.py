
from mrjob.job import MRJob
from mrjob.step import MRStep

class partC(MRJob):


    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 9:
                miner = fields[2]
                size = int(fields[4]) 
                yield (miner,size)
        except:
            pass

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper, combiner=self.combine_counts,
                reducer=self.reducer_sum_counts
            ),
            MRStep(
                reducer=self.reduce_sort_counts
            )
        ]

    def combine_counts(self, miner, counts):
        yield miner, sum(counts)

    def reducer_sum_counts(self, key, values):
        yield None, (sum(values), key)

    def reduce_sort_counts(self, _, size_counts):
        i = 0
        for count, key in sorted(size_counts, reverse=True):
            yield (key,int(count))

            i += 1
            if i >= 10:
                break

if __name__ == '__main__':
    partC.run()
