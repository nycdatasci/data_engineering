from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import random


class MRRandomSampling(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol
    
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--fraction', type=float)
        
    def mapper_init(self):
        if self.options.fraction > 1 or self.options.fraction < 0:
            raise ValueError('Invalid fraction value')
        
    def mapper(self, _, value):
        key = value.get('jobId', 0)
        if random.uniform(0, 1) < self.options.fraction:
            yield _, value


if __name__ == '__main__':
    MRRandomSampling.run()