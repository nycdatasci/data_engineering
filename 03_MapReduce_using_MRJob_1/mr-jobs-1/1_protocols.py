from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol


class MRTest(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, value):
        yield value.get('jobId', None), value.get('jobLocation', None)

        
if __name__ == '__main__':
    MRTest.run()