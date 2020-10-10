import os
import fcntl
import mmap
import threading
import contextlib

from .freelist import FreeList
from .page import page_from_data
from .tx import Tx
from .rwlock import RWLock
from .share import leafPageFlag, metaPageFlag, freelistPageFlag, \
    page_struct, meta_tuple, meta_struct


PAGESIZE = 4096


class BoltDB:

    def __init__(self, filename, readonly=False):
        self.filename = filename
        self.readonly = readonly
        self.fd = os.open(filename, os.O_RDWR | os.O_CREAT, 0o666)
        if readonly:
            fcntl.lockf(self.fd, fcntl.LOCK_SH)
        else:
            fcntl.lockf(self.fd, fcntl.LOCK_EX)
        self.datasz = os.fstat(self.fd).st_size
        if self.datasz == 0:
            self._init_db_file()
        self.mmap = memoryview(mmap.mmap(self.fd, self.datasz, access=mmap.ACCESS_WRITE))
        self.meta0 = meta_tuple._make(meta_struct.unpack(
            self.mmap[page_struct.size:page_struct.size+meta_struct.size]))
        self.pagesize = self.meta0.pageSize
        self.max_pgid = self.datasz // self.pagesize

        if readonly:
            self.freelist = None
        else:
            self.freelist = FreeList()
            self.freelist.read(self.page(self.meta().freelist))

        self.lock = threading.Lock()
        self.meta_lock = threading.Lock()
        self.mmap_lock = RWLock()

    def _init_db_file(self):
        buf = memoryview(bytearray(PAGESIZE*4))
        for i in range(2):
            p = page_from_data(buf[i*PAGESIZE:])
            p.id = i
            p.flags = metaPageFlag
            p.write_header()
            meta_struct.pack_into(
                buf, i*PAGESIZE+page_struct.size,
                0xED0CDAED, 2, PAGESIZE, 0, 3, 0, 2, 4, i, 0
            )

        i = 2
        p = page_from_data(buf[i*PAGESIZE:])
        p.id = i
        p.flags = freelistPageFlag
        p.write_header()

        i = 3
        p = page_from_data(buf[i*PAGESIZE:])
        p.id = i
        p.flags = leafPageFlag
        p.write_header()

        self.datasz = os.write(self.fd, buf)
        os.fsync(self.fd)

    def meta(self):
        self.meta0 = meta_tuple._make(meta_struct.unpack(
            self.mmap[page_struct.size:page_struct.size+meta_struct.size]))
        self.meta1 = meta_tuple._make(meta_struct.unpack(
            self.mmap[self.pagesize+page_struct.size:self.pagesize+page_struct.size+meta_struct.size]))
        return self.meta1 if self.meta1.txid > self.meta0.txid else self.meta0

    def begin(self, writable=False):
        if writable:
            if self.readonly:
                raise Exception("database is in read-only mode")
            self.lock.acquire()

        self.meta_lock.acquire()
        tx = Tx(self, writable)
        self.meta_lock.release()

        if not writable:
            self.mmap_lock.r_acquire()

        return tx

    @contextlib.contextmanager
    def update(self):
        tx = self.begin(True)
        try:
            yield tx
            tx.commit()
        except: # noqa
            self.freelist.rollback()
            raise
        finally:
            tx.close()

    @contextlib.contextmanager
    def view(self):
        tx = self.begin()
        try:
            yield tx
        finally:
            tx.close()

    def page(self, pgid):
        return page_from_data(self.mmap[self.pagesize*pgid:])

    def allocate(self, n):
        pgid = self.freelist.allocate(n)
        if pgid == 0:
            if (self.max_pgid + n) * self.pagesize > self.datasz:
                self.datasz = (self.max_pgid + n + 4) * self.pagesize
                os.ftruncate(self.fd, self.datasz)
                self.mmap_lock.w_acquire()
                self.mmap.release()
                self.mmap = memoryview(mmap.mmap(self.fd, self.datasz, access=mmap.ACCESS_WRITE))
                self.mmap_lock.w_release()
            pgid = self.max_pgid
            self.max_pgid += n
            for i in range(n):
                self.freelist.allocate_new(pgid+i)

        p = self.page(pgid)
        p.id = pgid
        p.overflow = n - 1
        return p

    def close(self):
        self.lock.acquire()
        self.meta_lock.acquire()
        self.mmap_lock.w_acquire()

        self.mmap_lock.w_release()
        self.meta_lock.release()
        self.lock.release()

        fcntl.lockf(self.fd, fcntl.LOCK_UN)
        self.mmap.release()
        os.close(self.fd)

    def __del__(self):
        self.close()
