import os
import unittest
import tempfile
import threading

from pybolt import BoltDB
from pybolt.rwlock import RWLock


class BRWLock(RWLock):
    def w_acquire(self):
        if not self.w_lock.acquire(timeout=0.01):
            raise Exception("blocked")


class BLock:
    def __init__(self):
        self.lock = threading.Lock()

    def acquire(self):
        if not self.lock.acquire(timeout=0.01):
            raise Exception("blocked")

    def release(self):
        return self.lock.release()


class TestRW(unittest.TestCase):

    def setUp(self):
        self.db = BoltDB(tempfile.mktemp())

    def tearDown(self):
        os.unlink(self.db.filename)

    def test_rw(self):
        # replace mmap_lock
        self.db.mmap_lock = BRWLock()
        wtx = self.db.begin(True)
        rtx = self.db.begin(False)
        wtx.bucket().put(b"foo", b"bar")
        self.assertIsNone(rtx.bucket().get(b"foo"))
        # writer will block on mmap
        with self.assertRaisesRegex(Exception, "blocked"):
            wtx.commit()
        wtx.close()
        rtx.close()

    def test_ww(self):
        self.db.lock = BLock()
        wtx = self.db.begin(True)
        # only one writer at a time
        with self.assertRaisesRegex(Exception, "blocked"):
            self.db.begin(True)
        wtx.close()
        with self.db.update() as tx:
            pass
