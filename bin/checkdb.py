#!/usr/bin/env python
from __future__ import print_function
import click

from csvdb import CsvDatabase

class RioDatabase(CsvDatabase):
    def __init__(self, pathname=None, load=True):
        super(RioDatabase, self).__init__(pathname=pathname, load=load, metadata=_RioMetadata)

@click.command()

# Positional argument
@click.argument('dbdir', type=click.Path(exists=True))

@click.option('--shapes', '-s', is_flag=True, default=False,
              help='Check the Shapes data. Default is to skip this check.')

def main(dbdir, shapes):
    print("DB: {}, shapes: {}".format(dbdir, shapes))

    db = CsvDatabase.get_database(dbdir, load=False)

    print(db)

if __name__ == '__main__':
    main()
