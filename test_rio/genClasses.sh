#!/bin/sh
#
# This is how genClasses.py was run to generate the schema.py for the test code
#
RIO_DIR=$HOME/repos/RIO
export PYTHONPATH=$PYTHONPATH:$RIO_DIR:$RIO_DIR/csvdb

../bin/genClasses.py -d ../test.csvdb -c test_rio.tst_database.DataObject -D test_rio.tst_database.TestDatabase -o schema.py
