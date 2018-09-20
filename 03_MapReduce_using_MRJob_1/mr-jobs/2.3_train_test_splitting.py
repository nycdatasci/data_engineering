from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import decimal
import hashlib


class MRTrainTestSplit(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol
    
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--split')
        self.add_passthru_arg('--test_size', type=float, default=0.3)
        
    def mapper_init(self):
        if self.options.split not in ('train', 'test'):
            raise ValueError('Invalid split value')
        if self.options.test_size > 1 or self.options.test_size < 0:
            raise ValueError('Invalid test size')
        
    def mapper(self, _, value):
        key = value.get('jobId', 0)
        include = self._sample(key=key, fraction=self.options.test_size)
        if include ^ (self.options.split=='train'):
            yield _, value
    
    def _sample(self, key, fraction=1):
        frac = decimal.Decimal(str(fraction)).as_tuple()
        numer = sum([v*10**i for i, v in enumerate(frac.digits[::-1])])
        denom = 10**(-frac.exponent)
        hash_val = hashlib.md5(str(key).encode()).hexdigest()
        return (int(hash_val, 16) % denom) < numer
    
        
if __name__ == '__main__':
    MRTrainTestSplit.run()