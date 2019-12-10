from __future__ import print_function
from os import path

from schema import *
from testdb import Test2Database

class Keyless(NoKey):
    def __init__(self, scenario=None):
        super(Keyless, self).__init__(scenario)
        filters = {'sensitivity': scenario}
        self.init_from_db(None, scenario, **filters)

def main():
    pathname = path.normpath(path.join(path.realpath(__file__), '..', '..','test2.csvdb'))
    db = Test2Database.get_database(pathname)

    obj = Keyless(scenario='S2')

    no_key = db.get_table("NO_KEY")
    print("\nNO_KEY\n", no_key.data)

if __name__ == '__main__':
    main()
