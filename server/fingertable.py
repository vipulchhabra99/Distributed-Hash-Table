
m = 7

class FingerTable:
    def __init__(self, my_id):
        self.table = list()
        for i in range(m):
            x = (2**i)
            entry = (my_id + x) % (2**m)
            self.table.append( [entry, None] )

    def get_table_enteries(self, index):
        return self.table[index]

    def set_successor(self, node, index):
        self.table[index][1] = node
    def get_successor(self, index):
        return self.table[index][1]