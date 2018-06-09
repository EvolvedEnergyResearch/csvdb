#!/usr/bin/env python
import click
from csvdb import CsvMetadata, validate_db, ValidationDataError

# TODO: need to generalize this. Store this info in the metadata.txt file? Could have a
# TODO: special case for rows that start with '_', e.g., "_data_tables,GEOGRAPHIES,..."
_DbMetadata = [
    CsvMetadata('GEOGRAPHIES', data_table=True),
    CsvMetadata('GEOGRAPHIES_SPATIAL_JOIN', data_table=True),
    CsvMetadata('CURRENCY_CONVERSION', data_table=True),
    CsvMetadata('INFLATION_CONVERSION', data_table=True),
]

@click.command()
@click.argument('dbdir', type=click.Path(exists=True))      # Positional argument

@click.option('--shapes/--no-shapes', default=False,
              help='Check the Shapes data. Default is to skip this check.')

def main(dbdir, shapes):
    try:
        validate_db(dbdir, shapes, metadata=_DbMetadata)
    except ValidationDataError as e:
        print(e)

if __name__ == '__main__':
    main()
