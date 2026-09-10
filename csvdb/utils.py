from .error import CsvdbException

def col_match(col, value):
    """Deprecated: the query-string form of a single filter condition.

    Kept only for callers outside this package. `filter_query` no longer builds a query
    string -- see the note there for why.
    """
    if value is None:
        return '{} is None'.format(col)

    if isinstance(value, (int,  float)):
        return '{} == {}'.format(col, value)

    return '{} == "{}"'.format(col, value)

def ensure_tuple(obj):
    if isinstance(obj, tuple):
        return obj
    else:
        return (obj,)

def filter_query(df, filters):
    """Rows of `df` matching every (column, value) in `filters`.

    Filters with a boolean mask rather than by building a string and handing it to
    `DataFrame.query`, which is what this did:

        conds = [col_match(attr, value) for attr, value in filters.items()]
        return df.query(' and '.join(conds))

    That routed every lookup in the library through pandas' expression parser, and made
    correctness depend on things that have nothing to do with the data:

    * A COLUMN NAME THAT IS ALSO A DATAFRAME ATTRIBUTE. `query('name == "diesel blend"')`
      asks the parser to resolve `name`, which is also an attribute on pandas objects.
      Unbackticked, whether it binds to the column is a parser detail that has changed
      between pandas versions. When it fails to bind to the column the condition stops
      discriminating and the "filtered" frame still holds every row -- so a lookup for one
      blend returns rows for several, and the caller reports a table whose attributes are
      not constant when the table is fine.
    * A VALUE CONTAINING A DOUBLE QUOTE closes the literal early. The rest of the value
      becomes expression syntax: at best a parse error, at worst a different comparison
      that silently matches the wrong rows. Nothing escapes the interpolation.
    * PARSER AND ENGINE VERSION. `query` picks numexpr or python by frame size and falls
      back per-operation, so the same filter on the same data can behave differently
      across pandas builds. A version change can move a failure from one table to the next
      without touching either.

    A mask has no parser, no engine and no identifier rules, so none of that applies. It
    is also faster: no expression to compile.

    Semantics are the ones the query string had. `None` matches null, since `is None` in a
    query expression tested for missingness rather than identity. Numbers compare as
    numbers; everything else compares as the string the interpolation would have produced,
    which keeps object columns holding mixed str types matching as before.
    """
    if not filters:
        return df

    mask = None
    for col, value in filters.items():
        series = df[col]
        if value is None:
            cond = series.isna()
        elif isinstance(value, bool):
            cond = series == value
        elif isinstance(value, (int, float)):
            cond = series == value
        else:
            cond = series == value
            if not cond.any() and series.dtype == object:
                # the query string compared against a literal, so a column holding
                # non-str objects (numpy.str_, bytes) matched by their text
                cond = series.astype(str) == str(value)
        mask = cond if mask is None else (mask & cond)

    return df[mask]

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
