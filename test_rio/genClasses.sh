#!/bin/sh
#
# Run genClasses run to generate the schema.py for the test code
#
genClasses -d ../test.csvdb -c test_rio.tst_database.DataObject -D test_rio.tst_database.TestDatabase -o schema.py
