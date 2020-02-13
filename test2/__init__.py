from .testdb import _Metadata, Test2Database

#
# These methods form the API between a subclass of CsvDatabase and the validation subsystem.
#
def get_metadata():
    return _Metadata

def database_class():
    return Test2Database
