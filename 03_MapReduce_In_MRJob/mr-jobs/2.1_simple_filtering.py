from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol


class MRSimpleFiltering(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol
    
    def mapper(self, _, value):
        title = value.get('title', '').lower()
        if title.find('data scientist') > -1:
            yield _, value


if __name__ == '__main__':
    MRSimpleFiltering.run()