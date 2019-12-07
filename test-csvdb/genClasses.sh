#!/bin/sh
#
# Run genClasses to create schema.py for the test database
#
genClasses --dbdir=../test.csvdb --data-superclass=csvdb.data_object.DataObject --database-class=test.tst_database.TestDatabase -o schema.py
