#!/usr/bin/env python
from csvdb.validation import main, main_fun

if __name__ == '__main__':
    main()

    # # Keep commented out. Just used for testing
    # main_fun(dbdir=r'C:\github\rio_db_world\database',
    #          pkg_name='RIO.riodb',
    #          all=False,
    #          trim_blanks=False,
    #          drop_empty_rows=False,
    #          drop_empty_cols=False,
    #          drop_empty=False,
    #          schema_file='schema.csv',
    #          create_schema=False,
    #          delete_orphans=True,
    #          update_schema=False,
    #          include_shapes=False,
    #          save_changes=True,
    #          check_unique=False,
    #          validate=False)