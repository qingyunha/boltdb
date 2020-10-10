from .node import Inode
from .share import leafPageFlag, freelistPageFlag, \
    page_tuple, page_struct, \
    leaf_elem_tuple, leaf_elem_struct, \
    branch_elem_tuple, branch_elem_struct


class Page:

    def __init__(self):
        self.id = 0
        self.flags = 0
        self.count = 0
        self.overflow = 0

        self.header = None
        self.data = None
        self.inodes = None

    def is_leaf(self):
        return bool(self.flags & leafPageFlag)

    def leaf_elems(self):
        if self.inodes is not None:
            return self.inodes
        elem_size = leaf_elem_struct.size
        self.inodes = []
        for i in range(self.count):
            record = self.data[i*elem_size:(i+1)*elem_size]
            e = leaf_elem_tuple._make(leaf_elem_struct.unpack(record))
            pos = e.pos + i * elem_size
            key = bytes(self.data[pos:pos+e.ksize])
            value = bytes(self.data[pos+e.ksize:pos+e.ksize+e.vsize])
            n = Inode(key, value, 0, e.flags)
            self.inodes.append(n)
        return self.inodes

    def branch_elems(self):
        if self.inodes is not None:
            return self.inodes
        elem_size = branch_elem_struct.size
        self.inodes = []
        for i in range(self.count):
            record = self.data[i*elem_size:(i+1)*elem_size]
            e = branch_elem_tuple._make(branch_elem_struct.unpack(record))
            pos = e.pos + i * elem_size
            key = bytes(self.data[pos:pos+e.ksize])
            n = Inode(key, b"", e.pgid, 0)
            self.inodes.append(n)
        return self.inodes

    def write_inodes(self, inodes):
        self.count = len(inodes)
        self.inodes = inodes
        if self.is_leaf():
            elem_size = leaf_elem_struct.size
        else:
            elem_size = branch_elem_struct.size
        off = elem_size * self.count
        for i in range(self.count):
            n = self.inodes[i]
            sz = len(n.key) + len(n.value)
            if self.is_leaf():
                pos = i * elem_size
                # leaf_elem_tuple = namedtuple("leaf_elem", "flags pos ksize vsize")
                b = leaf_elem_struct.pack(n.flags, off-pos, len(n.key), len(n.value))
                self.data[pos:pos+len(b)] = b
                self.data[off:off+len(n.key)] = n.key
                try:
                    self.data[off+len(n.key):off+len(n.key)+len(n.value)] = n.value
                except: # noqa
                    print(type(n.value), len(n.value))
                    raise
            else:
                pos = i * elem_size
                # branch_elem_tuple = namedtuple("branch_elem", "pos ksize pgid")
                b = branch_elem_struct.pack(off-pos, len(n.key), n.pgid)
                self.data[pos:pos+len(b)] = b
                self.data[off:off+len(n.key)] = n.key
                # print("write branch", n.key)
            off += sz
        # print("write page", self.id, self.flags, self.count)
        self.write_header()

    def write_header(self):
        b = page_struct.pack(self.id, self.flags, self.count, self.overflow)
        self.header[:] = b

    def free_ids(self):
        ids = []
        for i in range(self.count):
            ids.append(int.from_bytes(self.data[i*8:(i+1)*8], "little"))
        return ids

    def write_ids(self, ids):
        self.flags = freelistPageFlag
        self.count = len(ids)
        for i in ids:
            self.data[i*8:(i+1)*8] = i.to_bytes(8, "little")
        self.write_header()


def page_from_data(data):
    page = Page()
    p = page_tuple._make(page_struct.unpack(data[:page_struct.size]))
    page.id = p.id
    page.flags = p.flags
    page.count = p.count
    page.overflow = p.overflow

    page.header = data[:page_struct.size]
    page.data = data[page_struct.size:]

    page.from_data = True
    return page
