#!/bin/sh
#
# This is how genClasses.py was run to generate the schema.py for the test code
#
RIO_DIR=$HOME/repos/RIO
export PYTHONPATH=$PYTHONPATH:$RIO_DIR:$RIO_DIR/csvdb

../bin/genClasses.py --dbdir=../test.csvdb --data-superclass=test.tst_database.DataObject --database-class=test.tst_database.TestDatabase -o schema.py
