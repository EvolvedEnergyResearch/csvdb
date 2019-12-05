#!/usr/bin/env python
from __future__ import print_function
import click
import gzip
import importlib
import os
import pandas as pd
import re

from csvdb.database import SHAPE_DIR, CSV_PATTERN, ZIP_PATTERN
from csvdb.error import ValidationUsageError, CsvdbException

Tables_to_skip = ['GEOGRAPHIES_SPATIAL_JOIN']

DefaultSchemaFile = 'csvdb-schema.csv'

def mkdirs(newdir, mode=0o770):
    """
    Try to create the full path `newdir` and ignore the error if it already exists.
    """
    from errno import EEXIST

    try:
        os.makedirs(newdir, mode)
    except OSError as e:
        if e.errno != EEXIST:
            raise

def create_file_map(dbdir):
    file_map = {}  # maps table names => file names under the database root folder

    if not os.path.exists(dbdir):
        raise ValidationUsageError('Database path "{}" does not exist'.format(dbdir))

    if not os.path.isdir(dbdir):
        raise ValidationUsageError('Database path "{}" is not a directory'.format(dbdir))

    prefixLen = len(dbdir) + 1

    for dirpath, dirnames, filenames in os.walk(dbdir, topdown=False):
        if os.path.basename(dirpath) == SHAPE_DIR:
            continue

        for filename in filenames:
            if re.match(CSV_PATTERN, filename):
                basename = os.path.basename(filename)
                tblname = basename.split('.')[0]   # removes either .csv or .csv.gz
                abspath = os.path.abspath(os.path.join(dirpath, filename)).replace('\\', '/')
                file_map[tblname] = abspath[prefixLen:]     # save path relative to dbdir

    print("Found {} .CSV files for {}".format(len(file_map), dbdir))
    return file_map

def create_schema_file(dbdir, schema_file):
    file_map = create_file_map(dbdir)

    with open(schema_file, 'w') as schema:
        for tblname, csvFile in file_map.items():
            csvFile = csvFile.replace('\\', '/')

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
        relpath = source_cols.pop(0).replace('\\', '/')
        abspath = os.path.join(dbdir, relpath).replace('\\', '/')

        basename = os.path.basename(relpath)
        tblname = basename.split('.')[0]

        csvFile = file_map.get(tblname)

        if not (csvFile and os.path.exists(abspath)):
            print('Creating empty file {}'.format(relpath))
            if run:
                mkdirs(os.path.dirname(abspath))
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
            source_cols = map(str.strip, source_cols)
            target_cols = map(str.strip, target_cols)

            source_set = set(source_cols)
            target_set = set(target_cols)

            extra = target_set - source_set
            missing = source_set - target_set
            reorder = (source_set == target_set) and (source_cols != target_cols)

            if extra or missing or reorder:
                print('{}:'.format(relpath))
                extra   and print(' - dropping extra columns {}'.format(sorted(extra)))
                missing and print(' - adding missing cols {}'.format(sorted(missing)))
                reorder and print(' - reordering columns')

            if run:
                new = pd.DataFrame(columns=source_cols)     # all columns, in correct order
                old = pd.read_csv(abspath, index_col=None)
                old.columns = map(str.strip, old.columns)

                if len(old) > 0:
                    for col in source_set.intersection(target_set):
                        new[col] = old[col]

                new.to_csv(abspath, index=None)

@click.command()

@click.argument('dbdir', type=click.Path(exists=True))      # Positional argument

@click.option('--trim-blanks', '-b', is_flag=True, default=False,
              help='Trim blanks surrounding column names and data values')

@click.option('--delete', '-d', type=str, metavar='TABLE.COL=value',
              help='Delete a row from a table by key, including cascading deletes indicated in validation.csv. Argument must be of the form "TABLE.COL=value"')

@click.option('--drop-empty-rows', is_flag=True, default=False,
              help='Drop empty rows when they are found')

@click.option('--drop-empty-cols', is_flag=True, default=False,
              help='Drop empty columns when they are found')

@click.option('--drop-empty', '-e', is_flag=True, default=False,
              help='Shorthand for --drop-empty-rows --drop-empty-cols')

@click.option('--schema-file', '-s', default=DefaultSchemaFile, metavar='FILENAME',
              help='The name of the file containing schema meta info. Default is "{}".'.format(DefaultSchemaFile))

@click.option('--create-schema', '-c', is_flag=True, default=False,
              help='Save schema info in schema-file')

@click.option('--update-schema', '-u', is_flag=True, default=False,
              help='Update schema from info in schema-file')

@click.option('--run/--no-run', default=True,
              help='For update mode, whether to run the update or just show what would be done.')

@click.option('--update-csv/--no-update-csv', default=False,
              help='Whether to write changed data back to the CSV files. Default is --no-update-csv.')

@click.option('--check-unique', '-u', is_flag=True, default=False,
              help='Verify that all tables with keys have unique key values in all rows')

@click.option('--validate', '-v', type=str, metavar='PACKAGE',
              help='Validate the named python package, which must contain validate.csv.')

@click.option('--verbose', '-V', is_flag=True, default=False,
              help='Print confirmations of files whose schemas match')

def main(dbdir, trim_blanks, delete, drop_empty_rows, drop_empty_cols, drop_empty, schema_file,
         create_schema, update_schema, run, update_csv, check_unique, validate, verbose):

    if update_schema and create_schema:
        raise ValidationUsageError('Options --update-schema and --create-schema are mutually exclusive.')

    if drop_empty:
        drop_empty_rows = drop_empty_cols = True

    if validate:
        pkg_name = validate     # validate arg is a package name (str)

        try:
            package = importlib.import_module(pkg_name)
            cls = package.database_class()
            db = cls(pathname=dbdir, load=False)

            db.validate(pkg_name, update_csv,
                        trim_blanks=trim_blanks,
                        drop_empty_rows=drop_empty_rows,
                        drop_empty_cols=drop_empty_cols,
                        check_unique=check_unique)
        except CsvdbException as e:
            print(e)

    # TBD
    if delete:
        print("Got delete arg: '{}'".format(delete))

    if create_schema:
        create_schema_file(dbdir, schema_file)

    if update_schema:
        update_from_schema(dbdir, schema_file, run, verbose)


if __name__ == '__main__':
    main()
