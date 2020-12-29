from .tst_database import _Metadata, TestDatabase

#
# These methods form the API between a subclass of CsvDatabase and the validation subsystem.
#
def get_metadata():
    return _Metadata

def database_class():
    return TestDatabase
