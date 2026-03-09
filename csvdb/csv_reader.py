import gzip

import pandas as pd
import polars as pl


CSV_ENGINE_VALUES = ('auto', 'polars', 'pandas')

# Explicitly scoped to parse/schema style failures for auto-mode fallback.
_POLARS_FALLBACK_ERROR_NAMES = (
    'ComputeError',
    'SchemaError',
    'NoDataError',
    'InvalidOperationError',
    'ShapeError',
)

POLARS_FALLBACK_EXCEPTIONS = tuple(
    [getattr(pl.exceptions, name) for name in _POLARS_FALLBACK_ERROR_NAMES if hasattr(pl.exceptions, name)]
    + [UnicodeDecodeError]
)


class CsvReadResult(object):
    __slots__ = ['frame', 'engine_used', 'fallback_reason']

    def __init__(self, frame, engine_used, fallback_reason=None):
        self.frame = frame
        self.engine_used = engine_used
        self.fallback_reason = fallback_reason


def normalize_csv_engine(csv_engine):
    engine = (csv_engine or 'auto').strip().lower()
    if engine not in CSV_ENGINE_VALUES:
        raise ValueError("Invalid csv_engine '{}'. Expected one of {}".format(csv_engine, CSV_ENGINE_VALUES))
    return engine


def _polars_read_options(str_cols=None, options=None):
    read_options = {
        'null_values': [''],
        'try_parse_dates': False,
        'encoding': 'utf8-lossy',
        'infer_schema_length': 1000,
    }
    if options:
        read_options.update(options)

    schema_overrides = dict(read_options.pop('schema_overrides', {}) or {})
    if str_cols:
        for col in str_cols:
            schema_overrides[col] = pl.Utf8
    if schema_overrides:
        read_options['schema_overrides'] = schema_overrides

    return read_options


def _read_with_pandas(path, str_cols=None, is_gzip=False):
    converters = {col: str for col in str_cols} if str_cols else {}
    open_func = gzip.open if is_gzip else open

    if is_gzip:
        with open_func(path, 'r', encoding=None) as f:
            frame = pd.read_csv(f, index_col=None, converters=converters, na_values='', low_memory=False)
    else:
        with open_func(path, 'r', encoding='utf-8', errors='replace') as f:
            frame = pd.read_csv(f, index_col=None, converters=converters, na_values='', low_memory=False)

    return CsvReadResult(frame=frame, engine_used='pandas')


def _read_with_polars(path, str_cols=None, is_gzip=False, options=None):
    read_options = _polars_read_options(str_cols=str_cols, options=options)
    frame = pl.read_csv(path, **read_options)
    return CsvReadResult(frame=frame, engine_used='polars')


def _read_auto(path, str_cols=None, is_gzip=False, options=None):
    try:
        return _read_with_polars(path, str_cols=str_cols, is_gzip=is_gzip, options=options)
    except POLARS_FALLBACK_EXCEPTIONS as exc:
        result = _read_with_pandas(path, str_cols=str_cols, is_gzip=is_gzip)
        result.fallback_reason = '{}: {}'.format(type(exc).__name__, exc)
        return result


def read_csv_frame(path, csv_engine='auto', str_cols=None, is_gzip=False, options=None):
    engine = normalize_csv_engine(csv_engine)

    if engine == 'pandas':
        return _read_with_pandas(path, str_cols=str_cols, is_gzip=is_gzip)

    if engine == 'polars':
        return _read_with_polars(path, str_cols=str_cols, is_gzip=is_gzip, options=options)

    return _read_auto(path, str_cols=str_cols, is_gzip=is_gzip, options=options)
