from .bucket import Bucket
from .share import meta_struct


class Tx:

    def __init__(self, db, writable):
        self.db = db
        self.meta = db.meta()
        self.root = Bucket(self, self.meta.root_pgid)

        self.writable = writable
        self.pages = {}
        if self.writable:
            self.txid = self.meta.txid + 1

        self.closed = False

    def id(self):
        return self.txid

    def size(self):
        return self.meta.pgid * self.db.ps

    def bucket(self, name=None):
        if not name:
            return self.root
        else:
            return self.root.bucket(name)

    def create_bucket(self, name):
        return self.root.create_bucket(name)

    def delete_bucket(self, name):
        return self.root.delete_bucket(name)

    def cursor(self):
        return self.root.cursor()

    def commit(self):
        if not self.writable:
            self.close()
            return

        self.root.rebalance()

        self.root.spill()

        self.commit_freelist()

        self.write()

        self.write_meta()

        self.close()

    def page(self, pgid):
        if pgid in self.pages:
            return self.pages[pgid]
        return self.db.page(pgid)

    def allocate(self, n):
        p = self.db.allocate(n)
        self.pages[p.id] = p
        return p

    def commit_freelist(self):
        p = self.page(self.meta.freelist)
        self.db.freelist.write(p)

    def write(self):
        # for p in self.pages.values(): p.write_inodes()
        pass

    def write_meta(self):
        pgid = self.txid % 2
        p = self.db.page(pgid)
        new_meta = meta_struct.pack(
            self.meta.magic,
            self.meta.version,
            self.meta.pageSize,
            self.meta.flags,
            self.root.root_pgid,
            0,
            self.meta.freelist,
            self.db.max_pgid,
            self.txid,
            0,
        )
        p.data[:len(new_meta)] = new_meta
        self.db.mmap.obj.flush()

    def close(self):
        if self.closed:
            return
        self.closed = True

        if self.writable:
            self.db.lock.release()
        else:
            self.db.mmap_lock.r_release()

    def __del__(self):
        self.close()
