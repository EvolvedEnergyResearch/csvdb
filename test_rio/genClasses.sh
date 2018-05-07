#!/bin/sh
#
# This is how genClasses.py was run to generate the schema.py for the test code
#
../bin/genClasses.py -d ../test.csvdb -c test_rio.rio_database.DataMapper -D test_rio.rio_database.RioDatabase -o schema.py
