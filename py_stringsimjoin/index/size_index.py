from sys import maxsize

from py_stringsimjoin.index.index import Index


class SizeIndex(Index):
    def __init__(self, table, index_attr, tokenizer):
        self.table = table
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.index = {}
        self.min_length = maxsize
        self.max_length = 0
        super(self.__class__, self).__init__()

    def build(self):
        row_id = 0
        for row in self.table:
            index_string = row[self.index_attr]
            num_tokens = len(self.tokenizer.tokenize(index_string))
            
            if self.index.get(num_tokens) is None:
                self.index[num_tokens] = []

            self.index.get(num_tokens).append(row_id)

            if num_tokens < self.min_length:
                self.min_length = num_tokens
 
            if num_tokens > self.max_length:
                self.max_length = num_tokens

            row_id += 1  

        return True

    def probe(self, num_tokens):
        return self.index.get(num_tokens, [])
