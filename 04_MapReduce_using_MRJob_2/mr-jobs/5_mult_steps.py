from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol

import re


class MRIndexingSkills(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
    skillset = ['Java', 'JavaScript', 'C', 'C++', 'C#', 'Python', 'R', 'Bash',
                'MySQL', 'Postgresql', 'MongoDB', 'Html', 'Ruby', 'PHP', 
                'Swift', 'CSS', 'Julia', 'Golang', 'Github', 'Redis', 'Hadoop',
                'Spark', 'Hive', 'Pig', 'Spark', 'ElasticSearch', 'Kafka', 
                'Cassandra', 'AWS', 'GCP', 'Azure', 'Docker', 'kubernetes']
    
    def mapper_init_1(self):
        self.pattern = re.compile('|'.join(['(?<=\W){}(?=\W)'.format(re.escape(x)) for x in self.skillset]), 
                                  flags=re.IGNORECASE)
        
    def mapper_1(self, _, value):
        try:
            industry, description = value['industry'], value['description']
        except (KeyError, ValueError):
            pass
        else:
            skills = self.pattern.findall(description)
            for skill in skills:
                yield {'skill':skill.capitalize(), 'industry':industry}, 1
    
    def reducer_1(self, key, values):
        yield key, sum(values)
        
    def mapper_2(self, key, value):
        try:
            new_key = key['industry']
            new_value = {'skill':key['industry'], 'count':value}
        except KeyError:
            pass
        else:
            yield new_key, new_value
        
    def reducer_2(self, key, values): 
#         value = max(values, key=lambda v: v.get('count', 0))
        return key, max(values)
    
    def steps(self):
        return [
            # step 1
            MRStep(mapper_init=self.mapper_init_1,
                   mapper=self.mapper_1,
                   combiner=self.reducer_1,
                   reducer=self.reducer_1),
            # step 2
            MRStep(mapper=self.mapper_2,
                   reducer=self.reducer_2)
        ]

if __name__ == '__main__':
    MRIndexingSkills.run()