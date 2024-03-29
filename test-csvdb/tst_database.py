from csvdb import CsvMetadata, CsvDatabase

_Metadata = [
    CsvMetadata('ONE_COL'),

    CsvMetadata('EMISSIONS_PRICE',
                data_table=True,
                has_key_col=False),

    CsvMetadata('EMISSIONS_PRICE_BAD',
                data_table=True,
                has_key_col=False),

    CsvMetadata('GeographiesSpatialJoin',
                data_table=True),

    CsvMetadata('GeographyMapKeys',
                data_table=True),

    CsvMetadata('TECH_MAIN',
                has_key_col=False),

    CsvMetadata('NEW_TECH_CAPITAL_COST',
                key_col='name',
                df_filters=['cost_type', 'lifecycle'],
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes'],
                lowcase_cols=['sensitivity']),

    CsvMetadata('TECH_CAPITAL_COST',
                key_col='name',
                df_filters=['cost_type'],
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes'],
                lowcase_cols=['sensitivity']),

    CsvMetadata('TECH_CAPACITY_FACTOR',
                df_cols=['vintage', 'value', 'sensitivity'],
                drop_cols=['source', 'notes']),

    CsvMetadata('TECH_STARTUP_COST',
                drop_cols=['source', 'notes']),

    CsvMetadata('BLEND_CAPITAL_COST',
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('BLEND_EXISTING_STORAGE',
                df_cols=['sensitivity', 'gau', 'year', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('BLEND_CONNECTIONS'),

    CsvMetadata('CONVERSION_CAPITAL_COST',
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('CONVERSION_EFFICIENCY',
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes'])
]


class TestDatabase(CsvDatabase):
    def __init__(self, pathname=None, load=True):
        super(TestDatabase, self).__init__(pathname=pathname, load=load, metadata=_Metadata)
