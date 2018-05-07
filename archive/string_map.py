
class StringMap(object):
    """
    A simple class to map strings to integer IDs and back again.
    """
    instance = None

    @classmethod
    def getInstance(cls):
        if not cls.instance:
            cls.instance = cls()

        return cls.instance

    def __init__(self):
        self.text_to_id = {}     # maps text to integer id
        self.id_to_text = {}     # maps id back to text
        self.next_id = 1        # the next id to assign

    def store(self, text):
        # If already known, return it
        id = self.get_id(text, raise_error=False)
        if id is not None:
            return id

        id = self.next_id
        self.next_id += 1

        self.text_to_id[text] = id
        self.id_to_text[id] = text
        return id

    def get_id(self, text, raise_error=True):
        return self.text_to_id[text] if raise_error else (None if id is None else self.text_to_id.get(text, None))

    def get_text(self, id, raise_error=True):
        return self.id_to_text[id] if raise_error else (None if id is None else self.id_to_text.get(id, None))

