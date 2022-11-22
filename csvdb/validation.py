
import click
import gzip
import importlib
import os
import pandas as pd
import re
import pdb

from .database import SHAPE_DIR, CSV_PATTERN, ZIP_PATTERN
from .error import ValidationUsageError

Tables_to_skip = ['GEOGRAPHIES_SPATIAL_JOIN']

DefaultSchemaFile = 'schema.csv'

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

def git_repo_is_clean(dir, git_exe='git'):
    from subprocess import check_output, PIPE, CalledProcessError

    cwd = os.getcwd()
    try:
        os.chdir(dir)

        try:
            output = check_output([git_exe, 'status', '--short'], stderr=PIPE)
            if len(output.strip()) == 0:
                return True
            else:
                return False

        except CalledProcessError: # Not a git repo => treat as clean
            return True

        except:  # Failed to run git (FileNotFoundError on Windows or OSError on Unix-like)
            print("WARNING: '{}' not found".format(git_exe))
            return False
    finally:
        os.chdir(cwd)


def create_file_map(dbdir):
    file_map = {}  # maps table names => file names under the database root folder

    if not os.path.exists(dbdir):
        raise ValidationUsageError('Database path "{}" does not exist'.format(dbdir))

    if not os.path.isdir(dbdir):
        raise ValidationUsageError('Database path "{}" is not a directory'.format(dbdir))

    dbdir = os.path.abspath(dbdir)
    prefixLen = len(dbdir) + 1

    for dirpath, dirnames, filenames in os.walk(dbdir, topdown=False):
        if SHAPE_DIR in dirpath:
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

                with openFunc(abspath, 'r') as csv:    # N.B. binary mode doesn't translate line endings
                    header = csv.readline().strip()
                    if isinstance(header, bytes):
                        header = header.decode()
                    schema.write(csvFile + ',')         # insert CSV basename in first column
                    schema.write(header + '\n')         # ensure consistent line endings

def update_from_schema(dbdir, schema_file, run):
    file_map = create_file_map(dbdir)

    with open(schema_file, 'r') as schema:
        lines = schema.readlines()

    for line in lines:
        line = line.strip()
        source_cols = line.split(',')
        relpath = source_cols.pop(0).replace('\\', '/')
        abspath = os.path.join(dbdir, relpath).replace('\\', '/')

        basename = os.path.basename(relpath)
        tbl_name = basename.split('.')[0]

        csvFile = file_map.get(tbl_name)

        if not (csvFile and os.path.exists(abspath)):
            print('Creating empty file {}'.format(relpath))
            if run:
                mkdirs(os.path.dirname(abspath))
                with open(abspath, 'w') as f:
                    f.write(','.join(source_cols))
                    f.write('\n')

            continue

        openFunc = gzip.open if re.match(ZIP_PATTERN, abspath) else open
        with openFunc(abspath, 'r') as csv:
            header = csv.readline().strip()
            if isinstance(header, bytes):
                header = header.decode()

        target_cols = header.split(',')

        if target_cols != source_cols:
            source_cols = [col.strip() for col in source_cols]
            target_cols = [col.strip() for col in target_cols]

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
                old.columns = [col.strip() for col in old.columns]

                if len(old) > 0:
                    for col in source_set.intersection(target_set):
                        new[col] = old[col]

                new.to_csv(abspath, index=None)

@click.command()

# Required positional arguments
@click.argument('dbdir', type=click.Path(exists=True))
@click.argument('pkg_name', type=str)

@click.option('--all', '-a', is_flag=True, default=False,
              help='Shorthand for --trim-blanks --drop-empty --check-unique')

@click.option('--trim-blanks', '-b', is_flag=True, default=False,
              help='Trim blanks surrounding column names and data values')

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

@click.option('--delete-orphans', '-d', is_flag=True, default=False,
              help='Scan all tables for orphaned rows and delete them.')

@click.option('--update-schema', '-u', is_flag=True, default=False,
              help='Update schema from info in schema-file')

@click.option('--include-shapes', '-p', is_flag=True, default=False,
              help='Include shapes in the validation and error checking')

@click.option('--save-changes/--no-save-changes', default=False,
              help='Whether to write changed data back to the CSV files. Default is --no-save-changes.')

@click.option('--check-unique', '-u', is_flag=True, default=False,
              help='Verify that all tables with keys have unique key values in all rows')

@click.option('--validate', '-v', is_flag=True, default=False,
              help='Validate the database based on validation.csv in the named package.')

def main(dbdir, pkg_name, all, trim_blanks, drop_empty_rows, drop_empty_cols, drop_empty,
         schema_file, create_schema, delete_orphans, update_schema, include_shapes, save_changes,
         check_unique, validate):
    main_fun(dbdir, pkg_name, all, trim_blanks, drop_empty_rows, drop_empty_cols, drop_empty,
         schema_file, create_schema, delete_orphans, update_schema, include_shapes, save_changes,
         check_unique, validate)

def main_fun(dbdir, pkg_name, all=False, trim_blanks=False, drop_empty_rows=False, drop_empty_cols=False, drop_empty=False,
         schema_file=None, create_schema=False, delete_orphans=False, update_schema=False, include_shapes=False, save_changes=False,
         check_unique=False, validate=False):

    if update_schema and create_schema:
        raise ValidationUsageError('Options --update-schema and --create-schema are mutually exclusive.')

    if save_changes and not git_repo_is_clean(dbdir):
        print("WARNING: git repo is dirty; changes will NOT be saved.")
        save_changes = False

    if all:
        drop_empty = trim_blanks = check_unique = True

    if drop_empty:
        drop_empty_rows = drop_empty_cols = True

    if drop_empty_rows or drop_empty_cols or delete_orphans:
        validate = True

    if create_schema:
        create_schema_file(dbdir, schema_file)

    if update_schema:
        update_from_schema(dbdir, schema_file, save_changes)

    if validate:
        db = None
        try:
            package = importlib.import_module(pkg_name)
            cls = package.database_class()
            db = cls(pathname=dbdir, load=False)

        except Exception as e:
            print(e)

        if db:
            db.validate(pkg_name,
                        skip_dir='ShapeData',           # make these cmdline args?
                        skip_tables=['GEOGRAPHIES'],

                        save_changes=save_changes,
                        trim_blanks=trim_blanks,
                        drop_empty_rows=drop_empty_rows,
                        drop_empty_cols=drop_empty_cols,
                        check_unique=check_unique,
                        include_shapes=include_shapes,
                        delete_orphans=delete_orphans)
