
from mrjob.job import MRJob
from mrjob.step import MRStep

class partB3(MRJob):
    def mapper(self, _, line):
        try:
            if(len(line.split('\t'))==2):
                fields=line.split('\t')
                join_key = fields[0].strip('"\"')
                join_value= int(fields[1])
                if join_value!=0:
                    yield (None,(join_key, join_value))


        except:
            pass
            #do nothing
    def combiner(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("{} - {} ".format(value[0],value[1]),None)

            i += 1
            if i >= 10:
                break

if __name__ == '__main__':
    partB3.JOBCONF= {'mapreduce.job.reduces': '10' }
    partB3.run()

