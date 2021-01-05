#!/usr/bin/env python

from __future__ import print_function
import click
import inspect
import sys

from csvdb import CsvDatabase, CsvdbException, importFromDotSpec, camelCase, DataObject

def observeLinewidth(args, linewidth, indent=16):
    processed = ''
    unprocessed = ', '.join(args)
    spaces = ' ' * indent

    while len(unprocessed) > linewidth:
        pos = unprocessed[:linewidth].rfind(',')   # find last comma before linewidth
        processed += unprocessed[:pos + 1] + '\n' + spaces
        unprocessed = unprocessed[pos + 1:]

    processed += unprocessed
    return processed

StartOfFile = '''#
# This is a generated file. Manual edits may be lost!
#
import sys
from {superclassModule} import {superclassName} # superclass of generated classes

_Module = sys.modules[__name__]  # get ref to our own module object

'''

class ClassGenerator(object):
    def __init__(self, dbdir, dbclass, superclass, outfile, linewidth):
        self.db = db = dbclass.get_database(dbdir, load=False)
        self.superclassModule = superclass.__module__
        self.superclassName   = superclass.__name__
        self.outfile = outfile
        self.linewidth = linewidth
        self.generated = []
        self.tables = None
        self.all_tables = db.get_table_names()

    def generateClass(self, stream, table):
        print("Generating class for", table)

        self.generated.append(table)

        base_class = self.superclassName
        class_name = camelCase(table)

        stream.write('class {}({}):\n'.format(class_name, base_class))
        stream.write('    _instances_by_key = {}\n')

        db = self.db
        # if table == 'DEMAND_SERVICE_DEMAND':
        #     import pdb
        #     pdb.set_trace()

        tbl = db.get_table(table)
        md = tbl.metadata

        # Default values are computed in CsvTable.__init__()
        key_col     = md.key_col
        attr_cols   = md.attr_cols
        df_cols     = md.df_cols
        df_filters  = md.df_filters

        stream.write('    _table_name = "{}"\n'.format(table))
        stream.write('    _key_col = {!r}\n'.format(key_col))  # save as a class variable

        def write_class_var(name, col_or_cols):
            if isinstance(col_or_cols, (list, tuple)):
                col_strs = map(lambda s: '"{}"'.format(s), col_or_cols)
                stream.write('    {} = [{}]\n'.format(name, observeLinewidth(col_strs, self.linewidth, indent=12)))
            else:
                stream.write('    {} = "{}"\n'.format(name, col_or_cols))

        sorted_attrs = sorted(attr_cols)
        write_class_var('_cols', sorted_attrs)
        write_class_var('_df_cols', df_cols)
        write_class_var('_df_filters', df_filters)

        # We try 2 variants of data table names before giving up
        data_table = table + 'Data'
        if data_table not in self.all_tables:
            data_table = table + 'NewData'
            if data_table not in self.all_tables:
                data_table = ''

        stream.write('    _data_table_name = {!r}\n'.format(str(data_table) or None))
        stream.write('\n')

        params = [col + '=None' for col in sorted_attrs]
        params = observeLinewidth(params, self.linewidth)

        if key_col is None:
            stream.write('    def __init__(self, scenario):\n')
        else:
            stream.write('    def __init__(self, {}, scenario):\n'.format(key_col))

        stream.write('        {}.__init__(self, {}, scenario)\n'.format(base_class, key_col))
        stream.write('\n')
        stream.write('        {}._instances_by_key[self._key] = self\n'.format(class_name))
        stream.write('\n')

        for col in sorted_attrs:
            value = col if col == key_col else None
            stream.write('        self.{} = {}\n'.format(col, value))

        stream.write('\n')

        stream.write('    def set_args(self, scenario, {}):\n'.format(params))
        stream.write('        self.check_scenario(scenario)\n\n')

        for col in sorted_attrs:
            stream.write('        self.{col} = {col}\n'.format(col=col))

        args  = [col + '=' + col for col in sorted_attrs]
        args  = observeLinewidth(args, self.linewidth, indent=17)
        names = observeLinewidth(attr_cols, self.linewidth, indent=8)

        template = """
    def init_from_tuple(self, tup, scenario, **kwargs):    
        ({names},) = tup

        self.set_args(scenario, {args})

"""

        stream.write(template.format(names=names, args=args))


    def generateClasses(self):
        stream = open(self.outfile, 'w') if self.outfile else sys.stdout

        stream.write(StartOfFile.format(superclassModule=self.superclassModule, superclassName=self.superclassName))

        db = self.db

        # filter out tables that don't need classes
        tables = self.tables = db.tables_with_classes(include_on_demand=True)

        for name in tables:
            self.generateClass(stream, name)

        sys.stderr.write('Generated {} classes\n'.format(len(self.generated)))
        if stream != sys.stdout:
            stream.close()


def classFromDotSpec(spec):
    try:
        cls = importFromDotSpec(spec)
    except CsvdbException as e:
        raise click.UsageError(str(e))

    if not inspect.isclass(cls):
        raise click.UsageError('The specification "{}" does not refer to a class'.format(spec))

    return cls


@click.command()

@click.option('--dbdir', '-d', type=click.Path(exists=True, file_okay=False), default="database.csvdb",
              help='Path to the database directory. (Default is "database.csvdb")')

@click.option('--data-superclass', '-c', default='csvdb.data_object.DataObject',
              help='''
The superclass generated classes should inherit from, which must be "DataObject" or \
a subclass thereof. Format is a module "dot spec" where the final element is the name \
of the class. (Default: csvdb.data_object.DataObject)''')

@click.option('--database-class', '-D', default="csvdb.database.CsvDatabase",
              help='''
The name of the module defining the superclass for generated classes. Format \
is a module "dot spec" where the final element is the name of the class. \
(Default: csvdb.database.CsvDatabase)''')

@click.option('--outfile', '-o', default='',
              help='File to create. If not provided, generated classes are written to stdout.')

@click.option('--linewidth', '-l', default=90, type=int,
              help='Maximum line width. (Default: 90)')

def main(dbdir, data_superclass, database_class, outfile, linewidth):
    superclass = classFromDotSpec(data_superclass)
    dbclass    = classFromDotSpec(database_class)

    # Verify class relationships
    if not issubclass(superclass, DataObject):
        raise CsvdbException("Superclass {} must be a subclass of DataObject".format(data_superclass))

    if not issubclass(dbclass, CsvDatabase):
        raise CsvdbException("Database class {} must be a subclass of CsvDatabase".format(database_class))

    obj = ClassGenerator(dbdir, dbclass, superclass, outfile, linewidth)
    obj.generateClasses()

if __name__ == '__main__':
    main()
