from .error import CsvdbException

def col_match(col, value):
    """
    Creates a query string to match a column in a dataframe.
    """
    if value is None:
        return '{} is None'.format(col)

    if isinstance(value, (int,  float)):
        return '{} == {}'.format(col, value)

    return '{} == "{}"'.format(col, value)

def filter_query(df, filters):
    """
    Convert a dict of filters into a string query suitable for a DataFrame.
    """
    if not filters:
        return df

    conds = [col_match(attr, value) for attr, value in filters.items()]
    query = ' and '.join(conds)

    result = df.query(query)
    return result

def camelCase(s):
    """
    If a string has any underscores (e.g., 'Camel_case', change it to 'CamelCase'.
    """
    return s.title().replace('_', '') if '_' in s else s

def importFrom(modname, objname, asTuple=False):
    """
    Import `modname` and return reference to `objname` within the module.

    :param modname: (str) the name of a Python module
    :param objname: (str) the name of an object in module `modname`
    :param asTuple: (bool) if True a tuple is returned, otherwise just the object
    :return: (object or (module, object)) depending on `asTuple`
    """
    from importlib import import_module

    module = import_module(modname, package=None)
    obj = getattr(module, objname)
    return ((module, obj) if asTuple else obj)

def importFromDotSpec(spec):
    """
    Import an object from an arbitrary dotted sequence of packages, e.g.,
    "a.b.c.x" by splitting this into "a.b.c" and "x" and calling importFrom().

    :param spec: (str) a specification of the form package.module.object
    :return: none
    :raises PygcamException: if the import fails
    """
    modname, objname = spec.rsplit('.', 1)

    try:
        return importFrom(modname, objname)

    except ImportError:
        raise CsvdbException("Can't import '%s' from '%s'" % (objname, modname))
