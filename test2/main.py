from __future__ import print_function
from os import path

from schema import *
from testdb import Test2Database

class EmissionsCons(EmissionsConstraint):
    def __init__(self, scenario=None):
        super(EmissionsCons, self).__init__(scenario)
        filters = {'sensitivity': scenario}
        self.init_from_db(None, scenario, **filters)

def get_and_print(scenario):
    obj = EmissionsCons(scenario=scenario)
    print("\nEMISSIONS_CONSTRAINT ({})\n".format(scenario), obj.raw_values)

def main():
    pathname = path.normpath(path.join(path.realpath(__file__), '..', '..', 'test2.csvdb'))
    db = Test2Database.get_database(pathname)

    get_and_print('50% reduction by 2050')
    get_and_print('90% reduction by 2050')
    get_and_print('99% reduction by 2050')

    values = db.get_valid_options('TYPE', 'name', pkg_name='test2')
    print("Valid options for TYPE.name:", values)

if __name__ == '__main__':
    main()
