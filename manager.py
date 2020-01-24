import records as rd
import sqlite3 as ls
import yaml as ym
from littledb.errs import invalidateYAML
from littledb.connection import connection


class manager:
    def __init__(self, configFile='dbs.yml'):
        self.filename = configFile
        with open(self.filename, 'r') as f:
            self.dbs = list(ym.load(f,Loader=ym.FullLoader))
        if not isinstance(self.dbs, list):
            raise invalidateYAML

    def __str__(self):
        return '\r\n'.join([x['name']+' = '+x['path'] for x in self.dbs])
    def add(self, location,name):
        if location not in [x['path'] for x in self.dbs]:
            try:
                test = ls.connect(location)
            except:
                pass
            else:
                test.close()
                self.dbs.append({'path':location,'name':name})
                with open(self.filename, 'w') as f:
                    ym.dump(self.dbs, f)

    @staticmethod
    def gene(dblist, filename='dbs.yml'):
        with open(filename, 'w') as f:
            ym.dump(dblist, f)

    def __repr__(self):
        return 'manager for: '+'\r\n'.join([x['name']+' = '+x['path'] for x in self.dbs])

    def find_table(self, name: str) -> list:
        result = list()
        for i in self.dbs:
            if name in connection(i['path']).get_table_names():
                result.append(i['path'])
        return result


