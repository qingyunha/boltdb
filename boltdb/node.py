import functools
from bisect import bisect_left

from .share import page_struct, leaf_elem_struct, branch_elem_struct


@functools.total_ordering
class Inode:
    def __init__(self, key, value, pgid, flags):
        self.key = key
        self.value = value
        self.pgid = pgid
        self.flags = flags

    def __eq__(self, other):
        return self.key == other

    def __lt__(self, other):
        return self.key < other


class Node:

    def __init__(self, bucket):
        self.bucket = bucket
        self.is_leaf = False 
        self.unbalanced = False
        self.spilled = False
        self.key = None
        self.pgid = 0
        self.parent = None
        self.children = []
        self.inodes = []

    def root(self):
        if self.parent is None:
            return self
        return self.parent.root()

    def size(self):
        sz = page_struct.size
        elsz = leaf_elem_struct.size \
                if self.is_leaf else branch_elem_struct.size
        for n in self.inodes:
            sz += elsz + len(n.key) + len(n.value)
        return sz

    def child_at(self, index):
        if self.is_leaf:
            raise Exception("invalid child_at on a leaf node")
        return self.bucket.node(self.inodes[index].pgid, self)

    def child_index(self, n):
         return bisect_left(self.inodes, n.key)

    def num_children(self):
        return len(self.inodes)

    def next_sibling(self):
        if self.parent is None:
            return None
        index = self.parent.child_index(self)
        if index >= self.parent.num_children():
            return None
        return self.parent.child_at(index+1)

    def prev_sibling(self):
        if self.parent is None:
            return None
        index = self.parent.child_index(self)
        if index == 0:
            return None
        return self.parent.child_at(index-1)

    def put(self, old_key, new_key, value, pgid, flags):
        index = bisect_left(self.inodes, old_key)
        exact = self.inodes and index < len(self.inodes) and \
                self.inodes[index].key == old_key
        n = Inode(new_key, value, pgid, flags)
        if exact:
            self.inodes[index] = n
        else:
            self.inodes.insert(index, n)

    def delete(self, key):
        if not self.inodes:
            return
        index = bisect_left(self.inodes, key)
        if index >= len(self.inodes) or self.inodes[index] != key:
            return
        self.inodes.pop(index)
        self.unbalanced = True

    def read(self, p):
        if p.is_leaf():
            self.is_leaf = True
            self.inodes = p.leaf_elems()
        else:
            self.inodes = p.branch_elems()
        if len(self.inodes) > 0:
            self.key = self.inodes[0].key

    def write(self, p=None):
        if p is None:
            p, _ = self.bucket.page_node(self.pgid)
            if p is None:
                p = self.bucket.page or self.bucket.tx.page(self.pgid)
        if self.is_leaf:
            p.flags = 0x2
        else:
            p.flags = 0x1
        p.write_inodes(self.inodes)

    def spill(self):
        if self.spilled:
            return

        for c in self.children:
            c.spill()

        # print("spill page id", self.pgid, id(self), id(self.parent), self.size())
        # print("spill nodes ", len(nodes))
        tx = self.bucket.tx
        self.children = []
        nodes = self.split(tx.db.pagesize)
        for n in nodes:
            # if n.pgid == 0:
            #     p = self.bucket.tx.allocate((n.size()+4096-1)//4096)
            #     n.pgid = p.id
            # else:
            #     p = None
            if n.pgid > 0: tx.db.freelist.free(tx.page(n.pgid))
            p = self.bucket.tx.allocate((n.size()+tx.db.pagesize-1)//tx.db.pagesize)
            n.pgid = p.id
            n.write(p)
            n.spilled = True

            if n.parent is not None and len(n.inodes) > 0:
                key = n.key or n.inodes[0].key
                n.parent.put(key, n.inodes[0].key, b"", n.pgid, 0)
                n.key = n.inodes[0].key

        if self.parent is not None and self.parent.pgid == 0:
            self.children = []
            self.parent.spill()

    def split(self, pagesize):
        nodes = []
        node = self
        while node is not None:
            a, b = node.split_two(pagesize)
            nodes.append(a)
            node = b
        return nodes

    def split_two(self, pagesize):
        # print("split me", id(self), self.size())
        if len(self.inodes) <= 2 or self.size() < pagesize:
            return self, None

        # i = len(self.inodes) // 2
        i = self._split_index(pagesize*3/4)
        if self.parent is None:
            p = Node(self.bucket)
            p.children = [self]
            self.parent = p

        next = Node(self.bucket)
        next.is_leaf = self.is_leaf
        next.parent = self.parent
        next.parent.children.append(next)

        inodes = self.inodes
        next.inodes = inodes[i:]
        self.inodes = inodes[:i]
        return self, next

    def _split_index(self, threshold):
        sz = page_struct.size
        elsz = leaf_elem_struct.size \
                if self.is_leaf else branch_elem_struct.size
        for i, n in enumerate(self.inodes):
            sz += elsz + len(n.key) + len(n.value)
            if i >= 2 and sz > threshold:
                return i

    def rebalance(self):
        if not self.unbalanced:
            return
        self.unbalanced = False

        # print("rebalance", self.pgid, len(self.inodes))

        threshold = self.bucket.tx.db.pagesize/4
        if self.size() > threshold and len(self.inodes) > 2:
            return

        if self.parent is None:
            if self.num_children() == 0:
                self.is_leaf = True
            return

            # or this

            if not self.is_leaf and len(self.inodes) == 1:
                child = self.bucket.node(self.inodes[0].pgid, self)
                self.is_leaf = child.is_leaf
                self.inodes = child.inodes[:]
                self.children = child.children

                for i in self.inodes:
                    if i.pgid in self.bucket.nodes:
                        self.bucket.nodes[i.pgid].parent = self

                child.parent = None
                del self.bucket.nodes[child.pgid]
                child.free()

            return

        if self.num_children() == 0:
            self.parent.delete(self.key)
            self.parent.remove_child(self)
            del self.bucket.nodes[self.pgid]
            self.free()
            self.parent.rebalance()

    def remove_child(self, child):
        self.children.remove(child)

    def free(self):
        if self.pgid != 0:
            # freelist
            pass
        self.unbalanced = False
