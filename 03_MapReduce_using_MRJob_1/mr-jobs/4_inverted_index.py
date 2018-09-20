from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import re


class MRIndexingSkills(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
    skillset = ['Java', 'JavaScript', 'C', 'C++', 'C#', 'Python', 'R', 'Bash',
                'MySQL', 'Postgresql', 'MongoDB', 'Html', 'Ruby', 'PHP', 
                'Swift', 'CSS', 'Julia', 'Golang', 'Github', 'Redis', 'Hadoop',
                'Spark', 'Hive', 'Pig', 'Spark', 'ElasticSearch', 'Kafka', 
                'Cassandra', 'AWS', 'GCP', 'Azure', 'Docker', 'kubernetes']
    
    def mapper_init(self):
        self.pattern = re.compile('|'.join(['(?<=\W){}(?=\W)'.format(re.escape(x)) for x in self.skillset]), 
                                  flags=re.IGNORECASE)
        
    def mapper(self, _, value):
        try:
            jobId, description = value['jobId'], value['description']
        except (KeyError, ValueError):
            pass
        else:
            skills = self.pattern.findall(description)
            for skill in skills:
                yield skill.capitalize(), jobId
    
    def reducer(self, key, values):
        yield key, list(values)
        

if __name__ == '__main__':
    MRIndexingSkills.run()