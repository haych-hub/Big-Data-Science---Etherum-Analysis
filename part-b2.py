
from mrjob.job import MRJob

class partB2(MRJob):
    def mapper(self, _, line):
        try:
            if(len(line.split('\t'))==2):
                fields=line.split('\t')
                join_key = fields[0].strip('"\"\\\"')
                join_value= int(fields[1])
                
                yield (join_key, join_value)

            elif len(line.split(',')) == 5:
             fields = line.split(',')
             join_key = fields[0].strip('"\"\\\"')
             yield(join_key,(join_val,2))

        except:
            pass
            #do nothing
    def reducer_sum(self, company, values):
        years = []
        sector = 0

        for value in values:
            if value[1]==1:
                sector=value[0]
            elif value[1]==2:
               years.append(value[0])
        if sector > 0 and len(years) != 0:
            yield (company, sector)

if __name__ == '__main__':
    partB2.JOBCONF= {'mapreduce.job.reduces': '10' }
    partB2.run()

