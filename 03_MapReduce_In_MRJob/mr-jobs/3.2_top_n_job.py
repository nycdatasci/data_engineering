from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import os
import json

class MRTopNJob(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol
        
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--cache')
    
    def mapper_init(self):
        self.cache = list()
        with open(self.options.cache, 'r') as f:
            for line in f:
                self.cache.append(tuple(json.loads(line)))
        
    def mapper(self, _, value):
        try:
            max_ = float(value['estimatedSalary']['value']['maxValue'])
            min_ = float(value['estimatedSalary']['value']['minValue'])
        except (KeyError, ValueError):
            pass
        else:
            if (max_, min_) in self.cache:
                yield _, value

if __name__ == '__main__':
    MRTopNJob.run()