from csvdb import CsvMetadata, CsvDatabase

_Metadata = [
    CsvMetadata('INPUT'),

    CsvMetadata('HAS_KEY',
                key_col='name',
                drop_cols=['desc']),

    CsvMetadata('EMISSIONS_CONSTRAINT',
                has_key_col=False,
                df_cols=['sensitivity', 'year', 'gau', 'value']),

    CsvMetadata('TYPE'),

    CsvMetadata('SUBTYPE',
                df_cols=['input']),
]

class Test2Database(CsvDatabase):
    def __init__(self, pathname=None, load=True):
        super(Test2Database, self).__init__(pathname=pathname, load=load, metadata=_Metadata)
