
from mrjob.job import MRJob
import time

class partA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                time_epoch = int(fields[6])
                day = time.strftime("%m-%Y",time.gmtime(time_epoch)) #returns the month and year
                yield (day, 1)
        except:
            pass
            #do nothing

    def combiner(self, day, counts):
        yield (day, sum(counts))

    def reducer(self, day, counts):
         for a in count:
            print (",",a,end='')
            yield (day, sum(counts))



if __name__ == '__main__':
    partA.run()
