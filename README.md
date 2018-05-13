# **csvdb** -- A CSV Database

The csvdb package provides classes for interacting with data stored in a collection of CSV files.

The are two levels of access:

1. Dataframes holding the raw CSV data, and

2. Instances of classes generated from the CSV data files by `csvdb/bin/genClasses.py`. The classes are created with instance variables matching a declared subset of the CSV column names.

Details follow.

## CSV Metadata

The class `CsvMetadata` (see database.py) defines several data table characteristics that are used both to generate and load the data classes. The call to instantiate a `CsvDatabase` (or subclass) passes in a list of `CsvMetadata` instances.

The `CsvMetadata` constructor takes these args:

```
#!python

    def __init__(self, table_name, data_table=False, key_col=None, attr_cols=None,
                 df_cols=None, df_key_col=None, drop_cols=None, upcase_cols=None):
```
* `table_name` (str) The basename of a CSV datafile, e.g., for file `MY_DATA.csv`, the table name is `MY_DATA`.

* `data_table` (bool) If True, no class is generated for this table; the only processing is to load the CSV data
  into the `data` instance variable in a `CsvTable` when `db.get_table(tbl_name)` is called. All other metadata attributes are ignored.

* `key_col` (str) The column holding the main row identifier. Default is `"name"`.

* `attr_cols` (list of str) Attribute columns become instance variables in the generated class. If `None`, `attr_cols` is set to all of the raw data columns that are not specified in `df_cols` or `drop_cols`.

* `df_cols` (list of str) Identifies columns to select to create a DataFrame from rows matching `key_col`.
Defaults to the empty list.

* `df_key_col` (str) A column to combine with `key_col` to identify timeseries data to store in a `DataFrame`.

* `drop_cols` (list of str) Columns to ignore, i.e., metadata that is not processed by the application.
Defaults to the empty list.

* `upcase_cols` (list of str) Columns whose values should be forced to uppercase on reading. The `key_col`, if any, is automatically forced to uppercase, so it need not be specified (though it is ok to do so.)

## Class hierarchy for data classes

* `DataObject` (data_object.py) inherits from `object`. It provides the basic functionality
underlying the generated data classes, such as setting instance variables from data rows, and
generating timeseries `DataFrames`.

* Optional user-defined subclass of `DataObject`. Application-wide functionality shared by all data classes can be implemented in a subclass of `DataObject`, and the location should be provided as an argument to the `csvdb/bin/genClasses.py` script. The format is a string of the form `package_name.module_name.class_name`.

* Generated classes -- one per CSV file, except for CSV files listed in the `tables_without_classes` or `tables_to_ignore` arguments to the constructor for `CsvDatabase` (or subclass thereof.)

* Optional user-defined subclasses of generated classes, to add app-specific behavior.

## Generating classes -- schema.py

The script `csvdb/bin/genClasses.py` creates table-specific subclasses `DataObject` based on a "CSV database", i.e., a directory with multiple CSV files that contain the data. Re-run this script after adding CSV files to the database directory or after modifying any `CsvMetadata`.
