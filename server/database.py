class Database:
    def __init__(self):
        self.data = dict()
    
    def insert(self, key, value):
        self.data[key] = value
    
    def delete(self, key):

        if(self.isPresent(key) == True):
            del self.data[key]
            return True

        else:
            return False

    def get_all(self):
        return self.data
    
    def search(self, search_key):
        if search_key in self.data:
            return self.data[search_key]
        else:
            return None

    def update(self, key, value):

        self.data[key] = value

    def isPresent(self, key):

        if(key in self.data):
            return True

        else:
            return False