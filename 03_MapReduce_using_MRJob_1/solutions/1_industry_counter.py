from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol


class MRCounter(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, value):
        try:
            industry = value['industry']
        except KeyError:
            yield 'NA', 1
        else:
            yield industry, 1
    
    def reducer(self, key, values):
        yield key, sum(values)
    
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=self.reducer,
                       reducer=self.reducer)]
    
        
if __name__ == '__main__':
    MRCounter.run()