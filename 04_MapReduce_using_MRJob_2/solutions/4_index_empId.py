from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import re


class MRIndexingSkills(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
        
    def mapper(self, _, value):
        try:
            organization = value.get('hiringOrganization', {})
            emp = {'empId': value['empId'], 'name': organization.get('name', None)}
            job = {'jobId': value['jobId'], 'title': value.get('title', None)}
        except (KeyError, ValueError):
            pass
        else:
            yield emp, job
    
    def reducer(self, key, values):
        yield key, list(values)
        

if __name__ == '__main__':
    MRIndexingSkills.run()