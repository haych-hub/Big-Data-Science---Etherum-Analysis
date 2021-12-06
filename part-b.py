
from mrjob.job import MRJob
import time

class partA1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                value = int(fields[3])
                time_epoch = int(fields[6])
                day = time.strftime("%m-%Y",time.gmtime(time_epoch)) 
                yield (day, (value,1))
               
        except:
            pass

    def combiner(self, day, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield (day, (total, count))


    def reducer(self, day, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield (day, total/count)


if __name__ == '__main__':
    partA1.run()
