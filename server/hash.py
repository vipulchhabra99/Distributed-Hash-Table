import hashlib

m = 7
class Hasher:

    def hash(self, message):
        digest = hashlib.sha256(message.encode()).hexdigest()
        digest = int(digest, 16) % pow(2, m)
        return digest