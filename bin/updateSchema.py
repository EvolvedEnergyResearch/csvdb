#!/usr/bin/env python
from __future__ import print_function

import click
import gzip
import os
import pandas as pd
import re

from csvdb.database import SHAPE_DIR, CSV_PATTERN, ZIP_PATTERN
from csvdb.error import CsvdbException

Tables_to_skip = ['GEOGRAPHIES_SPATIAL_JOIN']

DefaultSchemaFile = 'csvdb-schema.csv'


def create_file_map(dbdir):
    file_map = {}  # maps table names => file names under the database root folder

    if not os.path.exists(dbdir):
        raise CsvdbException('Database path "{}" does not exist'.format(dbdir))

    if not os.path.isdir(dbdir):
        raise CsvdbException('Database path "{}" is not a directory'.format(dbdir))

    prefixLen = len(dbdir) + 1

    for dirpath, dirnames, filenames in os.walk(dbdir, topdown=False):
        if os.path.basename(dirpath) == SHAPE_DIR:
            continue

        for filename in filenames:
            if re.match(CSV_PATTERN, filename):
                basename = os.path.basename(filename)
                tblname = basename.split('.')[0]   # removes either .csv or .csv.gz
                abspath = os.path.abspath(os.path.join(dirpath, filename))
                file_map[tblname] = abspath[prefixLen:]     # save path relative to dbdir

    print("Found {} .CSV files for {}".format(len(file_map), dbdir))
    return file_map

def create_schema_file(dbdir, schema_file):
    file_map = create_file_map(dbdir)

    with open(schema_file, 'w') as schema:
        for tblname, csvFile in file_map.items():

            if not tblname in Tables_to_skip:
                openFunc = gzip.open if re.match(ZIP_PATTERN, csvFile) else open
                abspath = os.path.join(dbdir, csvFile)

                with openFunc(abspath, 'rb') as csv:    # N.B. binary mode doesn't translate line endings
                    header = csv.readline().strip()
                    schema.write(csvFile + ',')         # insert CSV basename in first column
                    schema.write(header + '\n')         # ensure consistent line endings

def update_from_schema(dbdir, schema_file, run, verbose):
    file_map = create_file_map(dbdir)

    with open(schema_file, 'r') as schema:
        lines = schema.readlines()

    for line in lines:
        line = line.strip()
        source_cols = line.split(',')
        relpath = source_cols.pop(0)
        abspath = os.path.join(dbdir, relpath)

        basename = os.path.basename(relpath)
        tblname = basename.split('.')[0]

        csvFile = file_map.get(tblname)

        if not (csvFile and os.path.exists(abspath)):
            print('Creating empty file {}'.format(relpath))
            if run:
                with open(abspath, 'w') as f:
                    f.write(','.join(source_cols))
                    f.write('\n')

            continue

        with open(abspath, 'r') as csv:
            header = csv.readline().strip()

        target_cols = header.split(',')

        if target_cols == source_cols:
            if verbose:
                print('{}: OK'.format(relpath))

        else:
            source_set = set(map(str.strip, source_cols))
            target_set = set(map(str.strip, target_cols))

            extra = target_set - source_set
            missing = source_set - target_set
            reorder = (source_set == target_set)  # we already know source_cols != target_cols

            print('{}:'.format(relpath))
            extra   and print(' - dropping extra columns {}'.format(sorted(extra)))
            missing and print(' - adding missing cols {}'.format(sorted(missing)))
            reorder and print(' - reordering columns')

            if run:
                new = pd.DataFrame(columns=source_cols)     # all columns, in correct order
                old = pd.read_csv(abspath, index_col=None)
                old.columns = map(str.strip, old.columns)   # strip whitespace

                for col in source_set.intersection(target_set):
                    new[col] = old[col]

                new.to_csv(abspath, index=None)

@click.command()

@click.argument('dbdir', type=click.Path(exists=True))      # Positional argument

@click.option('--schema-file', '-s', default=DefaultSchemaFile,
              help='The name of the file containing schema meta info. Default is "{}".'.format(DefaultSchemaFile))

@click.option('--save', 'mode', flag_value='save', default=True,
              help='Save schema info in schema-file')

@click.option('--update', 'mode', flag_value='update',
              help='Update schema from info in schema-file')

@click.option('--run/--no-run', default=True,
              help='For update mode, whether to run the update or just show what would be done.')

@click.option('--verbose', '-v', is_flag=True, default=False,
              help='Print confirmations of files whose schemas match')


def main(dbdir, schema_file, mode, run, verbose):
    if mode == 'save':
        create_schema_file(dbdir, schema_file)

    else: # mode is 'update'
        update_from_schema(dbdir, schema_file, run, verbose)


if __name__ == '__main__':
    main()
