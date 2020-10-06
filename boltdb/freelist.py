from .share import page_struct, freelistPageFlag


class FreeList:

    def __init__(self):
        self.ids = []
        self.pending = []
        self.allocs = []
        self.cache = set()

    def size(self):
        n = self.count()
        if n >= 0xffff:
            n += 1
        return page_struct.size + 8 * n

    def count(self):
        return self.free_count() + self.pending_count()

    def free_count(self):
        return len(self.ids)

    def pending_count(self):
        return len(self.pending_count)

    def allocate(self, n):
        if len(self.ids) == 0:
            return 0
        initial = previd = 0
        for i, id in enumerate(self.ids):
            if previd == 0 or id - previd != 1:
                initial = id
            if id - initial + 1 == n:
                if i + 1 == n:
                    self.ids = self.ids[i+1:]
                else:
                    del self.ids[i-n+1:i+1]
                for i in range(n):
                    self.cache.remove(i+initial)
                    self.allocs.append(i+initial)
                return initial
            previd = id
        return 0

    def allocate_new(self, pid):
        self.allocs.append(pid)

    def free(self, p):
        if p.id <= 1:
            raise Exception("cannot free page 0 or 1")
        if p.id in self.cache:
            raise Exception("page already freed")
        for i in range(p.overflow+1):
            self.pending.append(p.id+i)
            self.cache.add(p.id+i)

    def rollback(self):
        self.ids = sorted(self.ids + self.allocs)
        self.cache.clear()
        self.pending.clear()
        for id in self.ids:
            self.cache.add(id)

    def read(self, p):
        if p.flags != freelistPageFlag:
            raise Exception("invalid freelist page")
        self.ids = p.free_ids()
        self.cache = set()
        for id in self.ids:
            self.cache.add(id)

    def write(self, p):
        self.ids = sorted(self.ids+self.pending)
        self.allocs.clear()
        self.pending.clear()
        p.write_ids(self.ids)
