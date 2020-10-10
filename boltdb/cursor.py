from bisect import bisect_left


class ElemRef:

    def __init__(self, page, node, index):
        self.page = page
        self.node = node
        self.index = index

    def is_leaf(self):
        if self.node is not None:
            return self.node.is_leaf
        return self.page.is_leaf()

    def count(self):
        if self.node is not None:
            return len(self.node.inodes)
        return self.page.count


class Cursor:
    def __init__(self, bucket):
        self.bucket = bucket
        self.stack = []

    def first(self):
        self.stack = []
        p, n = self.bucket.page_node(self.bucket.root_pgid)
        self.stack.append(ElemRef(p, n, 0))
        self._first()
        k, v, flags = self.key_value()
        if flags & 0x1:
            return k, None
        return k, v

    def last(self):
        self.stack = []
        p, n = self.bucket.page_node(self.bucket.root_pgid)
        ref = ElemRef(p, n, 0)
        ref.index = ref.count() - 1
        self.stack.append(ref)
        self._last()
        k, v, flags = self.key_value()
        if flags & 0x1:
            return k, None
        return k, v

    def next(self):
        k, v, flags = self._next()
        if flags & 0x1:
            return k, None
        return k, v

    def __next__(self):
        if len(self.stack) == 0:
            k, v = self.first()
        else:
            k, v = self.next()
        if k is None:
            raise StopIteration
        return k, v

    def prev(self):
        pass

    def seek(self, key):
        k, v, flags = self._seek(key)

        ref = self.stack[-1]
        if ref.index >= ref.count():
            k, v, flags = self._next()

        if k is None:
            return None, None
        if flags & 0x1:
            return k, None
        return k, v

    def _first(self):
        # go to fisrt leaf
        while True:
            ref = self.stack[-1]
            if ref.is_leaf():
                break
            if ref.node is not None:
                pgid = ref.node.inodes[ref.index].pgid
            else:
                pgid = ref.page.branch_elems()[ref.index].pgid
            p, n = self.bucket.page_node(pgid)
            self.stack.append(ElemRef(p, n, 0))

    def _last(self):
        # go to last leaf
        while True:
            ref = self.stack[-1]
            if ref.is_leaf():
                break
            if ref.node is not None:
                pgid = ref.node.inodes[ref.index].pgid
            else:
                pgid = ref.page.branch_elems()[ref.index].pgid
            p, n = self.bucket.page_node(pgid)
            ref = ElemRef(p, n, 0)
            ref.index = ref.count() - 1
            self.stack.append(ref)

    def _next(self):
        while True:
            i = len(self.stack) - 1
            while i >= 0:
                ref = self.stack[i]
                if ref.index < ref.count() - 1:
                    ref.index += 1
                    break
                i -= 1

            if i == -1:
                return None, None, 0

            self.stack = self.stack[:i+1]
            self._first()

            if self.stack[-1].count() == 0:
                continue

            return self.key_value()

    def _seek(self, key):
        self.stack = []
        self._search(key, self.bucket.root_pgid)
        return self.key_value()

    def _search(self, key, pgid):
        p, n = self.bucket.page_node(pgid)
        ref = ElemRef(p, n, 0)
        self.stack.append(ref)

        if ref.is_leaf():
            if n is not None:
                inodes = n.inodes
            else:
                inodes = ref.page.leaf_elems()
            ref.index = bisect_left(inodes, key)
        else:
            if n is not None:
                inodes = n.inodes
            else:
                inodes = p.branch_elems()
            index = bisect_left(inodes, key)
            if index == len(inodes) or (index > 0 and inodes[index].key != key):
                index -= 1
            ref.index = index
            self._search(key, inodes[index].pgid)

    def key_value(self):
        ref = self.stack[-1]

        if ref.count() == 0 or ref.index >= ref.count():
            return None, None, 0

        if ref.node is not None:
            n = ref.node.inodes[ref.index]
        else:
            n = ref.page.leaf_elems()[ref.index]

        return n.key, n.value, n.flags

    def node(self):
        ref = self.stack[-1]
        if ref.node is not None and ref.is_leaf():
            return ref.node

        n = self.stack[0].node
        if n is None:
            n = self.bucket.node(self.stack[0].page.id, None)

        for ref in self.stack[:-1]:
            n = n.child_at(ref.index)

        return n
