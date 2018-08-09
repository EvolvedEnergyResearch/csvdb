#!/usr/bin/env python
from __future__ import print_function

import click
from glob import glob
import os
import pandas as pd
import shutil

DefaultSchemaFile = 'csvdb-schema.csv'

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
    csvFiles = glob(os.path.join(dbdir, '*.csv'))

    if mode == 'save':
        with open(schema_file, 'w') as schema:
            for csvFile in csvFiles:
                basename = os.path.basename(csvFile)
                with open(csvFile, 'r') as csv:
                    header = csv.readline()
                    schema.write(basename + ',')        # insert CSV basename in first column
                    schema.write(header)

    else: # mode is 'update'
        with open(schema_file, 'r') as schema:
            lines = schema.readlines()

        for line in lines:
            line = line.strip()
            source_cols = line.split(',')
            basename = source_cols.pop(0)

            csvFile = os.path.join(dbdir, basename)

            if not os.path.exists(csvFile):
                print('Creating empty file {}'.format(basename))
                if run:
                    with open(csvFile, 'w') as f:
                        f.write(','.join(source_cols))
                        f.write('\n')

                continue

            with open(csvFile, 'r') as csv:
                header = csv.readline().strip()

            target_cols = header.split(',')
            if target_cols == source_cols:
                if verbose:
                    print('{}: OK'.format(basename))

            else:
                source_set = set(source_cols)
                target_set = set(target_cols)
                extra   = target_set - source_set
                missing = source_set - target_set
                reorder = (source_set == target_set) # we already know source_cols != target_cols

                print('{}:'.format(basename))
                extra   and print(' - dropping extra columns {}'.format(sorted(extra)))
                missing and print(' - adding missing cols {}'.format(sorted(missing)))
                reorder and print(' - resetting column order')

                if run:
                    shutil.copy2(csvFile, csvFile + '~')        # create backup file

                    new = pd.DataFrame(columns=source_cols)     # all columns, in correct order
                    old = pd.read_csv(csvFile, index_col=None)

                    for col in source_set.intersection(target_set):
                        new[col] = old[col]

                    new.to_csv(csvFile, index=None)

if __name__ == '__main__':
    main()
