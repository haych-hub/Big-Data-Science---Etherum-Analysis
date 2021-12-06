
from mrjob.job import MRJob

class partB1(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                toAddress = fields[2]
                value = int(fields[3])
                if value == 0:
                    pass
                else:
                	yield (toAddress,value)
        except:
            pass
            #do nothing
    def combiner(self, toAddress, counts):
        yield (toAddress, sum(counts))

    def reducer(self,toAddress, counts):
        yield (toAddress, sum(counts))

if __name__ == '__main__':
    partB1.JOBCONF= {'mapreduce.job.reduces': '10' }
    partB1.run()
