#!/usr/bin/env python
import click
from csvdb import CsvMetadata, validate_db, ValidationDataError
import importlib
import pdb

# TODO: need to generalize this. Store this info in the metadata.txt file? Could have a
# TODO: special case for rows that start with '_', e.g., "_data_tables,GEOGRAPHIES,..."
_DataTables = ['GEOGRAPHIES', 'GEOGRAPHIES_SPATIAL_JOIN', 'CURRENCY_CONVERSION', 'INFLATION_CONVERSION']

_DbMetadata = [CsvMetadata(name, data_table=True) for name in _DataTables]

@click.command()
@click.argument('dbdir', type=click.Path(exists=True))      # Positional argument
@click.argument('package', type=str)
@click.option('--update/--no-update', default=False, help='Whether to write changed data back to the CSV files. Default is --no-update.')
def main(dbdir, package, update):
    try:
        package = importlib.import_module(package)
        validate_db(dbdir, package.__path__[0], update, _DbMetadata)
    except ValidationDataError as e:
        print(e)

if __name__ == '__main__':
    main()
