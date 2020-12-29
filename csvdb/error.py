#
# Error classes
#

class CsvdbException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class RowNotFound(CsvdbException):
    def __init__(self, table, key):
        msg = "Row not found for key {} in table '{}'".format(key, table)
        super(RowNotFound, self).__init__(msg)

class MissingKeyColumn(CsvdbException):
    def __init__(self, table, col):
        msg = 'Key column {} is not present in table {}'.format(col, table)
        super(MissingKeyColumn, self).__init__(msg)

class MissingKeyValue(CsvdbException):
    def __init__(self, table, col):
        msg = 'Key column {} in table {} is missing one or more values'.format(col, table)
        super(MissingKeyValue, self).__init__(msg)

class DuplicateRowsFound(CsvdbException):
    def __init__(self, table, key):
        msg = "Duplicate rows found for key '{}' in table '{}'".format(key, table)

        super(DuplicateRowsFound, self).__init__(msg)

class UnknownDataClass(CsvdbException):
    def __init__(self, classname):
        msg = 'Unknown data classname "{}"'.format(classname)
        super(UnknownDataClass, self).__init__(msg)

class MissingParentIdColumn(CsvdbException):
    def __init__(self, table):
        msg = 'Table "{}" has no known parent ID column'.format(table)
        super(MissingParentIdColumn, self).__init__(msg)

class SubclassProtocolError(CsvdbException):
    def __init__(self, cls, method):
        msg = 'Class "{}" fails to implement method "{}"'.format(cls.__name__, method)
        super(SubclassProtocolError, self).__init__(msg)

class ValidationFormatError(Exception):
    def __init__(self, msg):
        msg = "Format error in validation.csv: {}".format(msg)
        super(ValidationFormatError, self).__init__(msg)

class ValidationUsageError(CsvdbException):
    def __init__(self, msg):
        super(ValidationUsageError, self).__init__(msg)

class ScenarioFileError(CsvdbException):
    def __init__(self, pathname, msg):
        msg = pathname + ': ' + msg
        super(ScenarioFileError, self).__init__(msg)
