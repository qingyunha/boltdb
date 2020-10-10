from .cursor import Cursor
from .page import page_from_data
from .node import Node
from .share import bucket_tuple, bucket_struct, bucketLeafFlag


class Bucket:

    def __init__(self, tx, root_pgid):
        self.tx = tx
        self.root_pgid = root_pgid
        self.name = ""

        self.root_node = None
        self.page = None

        self.nodes = {}
        self.pages = {}

        self.sub_buckets = {}

    def cursor(self):
        return Cursor(self)

    def __iter__(self):
        return self.cursor()

    def get(self, key):
        k, v = self.cursor().seek(key)
        if k != key:
            return None
        return v

    def put(self, key, value):
        if not self.tx.writable:
            raise Exception("cannot write in readonly tx")
        c = self.cursor()
        k, v = c.seek(key)
        if k == key and v is None:
            raise Exception("cannot write sub bucket")
        c.node().put(key, key, value, 0, 0)

    def delete(self, key):
        if not self.tx.writable:
            raise Exception("cannot write in readonly tx")
        c = self.cursor()
        k, _, flag = c._seek(key)
        if k != key:
            return
        if flag & bucketLeafFlag:
            raise Exception("cannot delete sub bucket")
        c.node().delete(key)

    def bucket(self, name):
        if name in self.sub_buckets:
            return self.sub_buckets[name]
        c = self.cursor()
        k, v, flags = c._seek(name)
        if k != name or flags & bucketLeafFlag == 0:
            return None
        b = self._open_bucket(v)
        b.name = name
        self.sub_buckets[name] = b
        return b

    def _open_bucket(self, value):
        bsize = bucket_struct.size
        b = bucket_tuple._make(bucket_struct.unpack(value[:bsize]))
        b = Bucket(self.tx, b.root)
        if b.root_pgid == 0:
            b.page = page_from_data(value[bsize:])
        return b

    def create_bucket(self, name):
        if not self.tx.writable:
            raise Exception("cannot write in readonly tx")
        c = self.cursor()
        k, _, flags = c._seek(name)
        if k == name:
            if flags & bucketLeafFlag:
                raise Exception("bucket already exists")
            raise Exception("incompatible value")

        b = Bucket(self.tx, 0)
        b.root_node = Node(self)
        b.root_node.is_leaf = True
        value = b.inline_value()

        c.node().put(name, name, value, 0, bucketLeafFlag)
        self.page = None
        return self.bucket(name)

    def delete_bucket(self, name):
        if not self.tx.writable:
            raise Exception("cannot write in readonly tx")

        child = self.bucket(name)
        if child is None:
            raise Exception("bucket not exists")

        del self.sub_buckets[name]

        c = self.cursor()
        c._seek(name)
        c.node().delete(name)

        # TODO free pages

    def page_node(self, pgid):
        if self.root_pgid == 0:
            if self.root_node is not None:
                return None, self.root_node
            return self.page, None

        if pgid in self.nodes:
            return None, self.nodes[pgid]

        if pgid in self.pages:
            return self.pages[pgid], None

        page = self.tx.page(pgid)
        self.pages[pgid] = page
        return page, None

    def node(self, pgid, parent):
        if pgid in self.nodes:
            return self.nodes[pgid]

        n = Node(self)
        n.parent = parent
        n.pgid = pgid
        if parent is None:
            self.root_node = n
        else:
            parent.children.append(n)
        p = self.page or self.pages.get(pgid) or self.tx.page(pgid)
        n.read(p)
        self.nodes[pgid] = n
        return n

    def inlineable(self):
        n = self.root_node
        if n is None or not n.is_leaf:
            return False
        # size = page_struct.size
        size = 16
        for i in n.inodes:
            size += 8 + len(i.key) + len(i.value)
            if i.flags * bucketLeafFlag:
                return False
            if size > 1024:
                return False
        return True

    def inline_value(self):
        n = self.root_node
        value = memoryview(bytearray(bucket_struct.size+n.size()))
        bucket_struct.pack_into(value, 0, 0, 0)
        p = page_from_data(value[bucket_struct.size:])
        n.write(p)
        return value.obj

    def spill(self):
        for name, child in self.sub_buckets.items():
            if child.inlineable():
                value = child.inline_value()
            else:
                child.spill()
                value = bucket_struct.pack(child.root_pgid, 0)

            if child.root_node is None:
                continue

            c = self.cursor()
            k, _, flags = c._seek(name)
            if k != name:
                raise Exception("misplaced bucket header")
            if flags & bucketLeafFlag == 0:
                raise Exception("unexpected bucket header flag")
            c.node().put(name, name, value, 0, bucketLeafFlag)

        if self.root_node is None:
            return

        self.root_node.spill()
        self.root_node = self.root_node.root()
        self.root_pgid = self.root_node.pgid

    def rebalance(self):
        for n in list(self.nodes.values()):
            n.rebalance()
        for child in self.sub_buckets.values():
            child.rebalance()

    def __del__(self):
        # self.close()
        pass
