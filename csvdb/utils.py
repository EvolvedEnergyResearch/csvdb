import weakref
import numpy as np
import pandas as pd
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

    entry = _ROW_INDEX.get(id(df))
    if entry is not None and entry.key_col in filters and entry.valid_for(df):
        rows = entry.filter(df, filters)
        if rows is not None:
            return rows

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

class _RowIndex(object):
    """Row positions of a table's data frame grouped by its key column, built on first lookup.

    `filter_query` uses it to restrict a lookup to the rows of one key before applying the
    other filters, so a lookup costs O(rows for that key) instead of O(table rows). The result
    is the same frame `df[mask]` returns: `df[mask]` is `df._take_with_is_copy(mask.nonzero())`,
    and the positions here are ascending, so taking them gives identical rows, order, index and
    dtypes.

    The one place the full-table mask depends on rows outside the key is the text fallback in
    `filter_query`: it compares by str() only when *no row in the whole column* equals the value.
    That test is answered here from a per-column set of the column's distinct values, so the
    fallback fires exactly when it did before.

    Registered for a frame by `register_row_index`; ignored if the frame object, its shape or its
    columns have changed since registration.
    """
    __slots__ = ('df_ref', 'key_col', 'shape', 'columns', 'positions', 'value_sets', '__weakref__')

    def __init__(self, df, key_col):
        self.df_ref = weakref.ref(df)
        self.key_col = key_col
        self.shape = df.shape
        self.columns = tuple(df.columns)
        self.positions = None
        self.value_sets = {}

    def valid_for(self, df):
        return self.df_ref() is df and df.shape == self.shape and tuple(df.columns) == self.columns

    def _build(self, df):
        col = df[self.key_col]
        values = col.to_numpy()
        # Only index a column whose every value is a python str: then a dict lookup by a str
        # value gives exactly the rows `col == value` selects.
        if col.dtype != object or not all(type(v) is str for v in values):
            self.positions = False
            return
        codes, uniques = pd.factorize(values)
        order = np.argsort(codes, kind='stable')
        bounds = np.searchsorted(codes[order], np.arange(len(uniques) + 1))
        self.positions = {u: order[bounds[i]:bounds[i + 1]] for i, u in enumerate(uniques)}

    def _value_set(self, df, col):
        vs = self.value_sets.get(col)
        if vs is None:
            vs = self.value_sets[col] = frozenset(v for v in df[col].to_numpy() if isinstance(v, str))
        return vs

    def filter(self, df, filters):
        key_value = filters[self.key_col]
        if not isinstance(key_value, str):
            return None
        if self.positions is None:
            self._build(df)
        if self.positions is False:
            return None
        pos = self.positions.get(key_value, _EMPTY_POSITIONS)
        if len(pos) == 0:
            # full path: no exact match, the str() fallback on an all-str column matches nothing too
            return df._take_with_is_copy(_EMPTY_POSITIONS)
        others = [(c, v) for c, v in filters.items() if c != self.key_col]
        if not others:
            return df._take_with_is_copy(pos)
        sub = df.take(pos)
        mask = None
        for col, value in others:
            series = sub[col]
            if value is None:
                cond = series.isna()
            elif isinstance(value, (bool, int, float)):
                cond = series == value
            else:
                cond = series == value
                if series.dtype == object:
                    if isinstance(value, str):
                        no_exact_match_in_table = value not in self._value_set(df, col)
                    else:
                        return None  # rare value type: let the full-table path decide
                    if no_exact_match_in_table:
                        cond = series.astype(str) == str(value)
            mask = cond if mask is None else (mask & cond)
        return df._take_with_is_copy(pos[mask.to_numpy()])


_EMPTY_POSITIONS = np.empty(0, dtype=np.intp)
_ROW_INDEX = {}  # id(frame) -> _RowIndex


def register_row_index(df, key_col):
    """Let filter_query look up rows of `df` by `key_col` through a key index."""
    if df is None or key_col is None or key_col not in df.columns:
        return
    entry = _RowIndex(df, key_col)
    k = id(df)
    _ROW_INDEX[k] = entry
    weakref.finalize(df, _ROW_INDEX.pop, k, None)


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
