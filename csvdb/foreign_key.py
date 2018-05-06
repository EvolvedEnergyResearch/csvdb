from collections import defaultdict

class ForeignKey(object):
    """"
    A simple data-only class to store foreign key information
    """
    __slots__ = ['table_name', 'column_name', 'foreign_table_name', 'foreign_column_name']

    # dict keyed by parent table name, value is list of ForeignKey instances
    fk_by_parent = defaultdict(list)

    # likewise, but keyed by child
    fk_by_child  = defaultdict(list)

    def __init__(self, tbl_name, col_name, for_tbl_name, for_col_name):
        self.table_name = tbl_name
        self.column_name = col_name
        self.foreign_table_name  = for_tbl_name
        self.foreign_column_name = for_col_name

        ForeignKey.fk_by_child[tbl_name].append(self)
        ForeignKey.fk_by_parent[for_tbl_name].append(self)

    def __str__(self):
        return "<ForeignKey {}.{} -> {}.{}>".format(self.table_name, self.column_name,
                                                    self.foreign_table_name, self.foreign_column_name)

    @classmethod
    def get_fkeys_by_child(cls, tbl_name):
        fkeys = ForeignKey.fk_by_child[tbl_name]
        return fkeys

    @classmethod
    def get_fkeys_by_parent(cls, tbl_name):
        fkeys = ForeignKey.fk_by_parent[tbl_name]
        return fkeys

    @classmethod
    def get_fk(cls, tbl_name, col_name):
        fkeys = cls.get_fkeys_by_child(tbl_name)

        for obj in fkeys:
            if obj.column_name == col_name:
                return obj

        return None
