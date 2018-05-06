#
# Example of a subclass of CsvDatabase that adds metadata.
#
from csvdb import CsvDatabase, CsvMetadata, DataObject

_RioMetadata = [
    CsvMetadata('TECH_MAIN'),  # all defaults apply

    CsvMetadata('TECH_CAPITAL_COST',
                key_col='name',
                df_key_col='cost_type',
                df_cols=['vintage', 'gau', 'sensitivity', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('TECH_CAPACITY_FACTOR',
                df_cols=['vintage', 'value', 'sensitivity'],
                drop_cols=['source', 'notes']),

    CsvMetadata('TECH_STARTUP_COST',
                drop_cols=['source', 'notes'])
]

class RioDatabase(CsvDatabase):
    def __init__(self, pathname=None, load=True):
        super(RioDatabase, self).__init__(pathname=pathname, load=load, metadata=_RioMetadata)

# Superclass to generated classes in schema.py. Just a placeholder for now,
# but could house datamapfunctions-type of functionality here.
class RioData(DataObject):
    def __init__(self, key, scenario):
        super(RioData, self).__init__(key, scenario)

        # TBD: may not be needed here
        # ivars set in create_index_levels
        # self.column_names = None
        # self.index_levels = None
        # self.df_index_names = None
