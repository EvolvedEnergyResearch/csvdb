#!/usr/bin/env python
import click
from csvdb import CsvMetadata, validate_db, ValidationDataError

# TODO: need to generalize this. Store this info in the metadata.txt file? Could have a
# TODO: special case for rows that start with '_', e.g., "_data_tables,GEOGRAPHIES,..."
_DataTables = ['GEOGRAPHIES', 'GEOGRAPHIES_SPATIAL_JOIN', 'CURRENCY_CONVERSION', 'INFLATION_CONVERSION']

_DbMetadata = [CsvMetadata(name, data_table=True) for name in _DataTables]

@click.command()
@click.argument('dbdir', type=click.Path(exists=True))      # Positional argument

@click.option('--force/--no-force', default=False,
              help='Force a check of all database files. Default file modification' 
              ' times and time of last check to determine what needs further checking.')

@click.option('--update/--no-update', default=False,
              help='Whether to write changed data back to the CSV files. Default is --no-update.')

@click.option('--shapes/--no-shapes', default=False,
              help='Check the Shapes data. Default is to skip this check.')

def main(dbdir, force, update, shapes):
    try:
        validate_db(dbdir, update, shapes, force, metadata=_DbMetadata)
    except ValidationDataError as e:
        print(e)

if __name__ == '__main__':
    main()
