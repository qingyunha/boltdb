import os
import unittest
import tempfile

from boltdb import BoltDB


class TestFree(unittest.TestCase):

    def setUp(self):
        self.db = BoltDB(tempfile.mktemp())

    def tearDown(self):
        os.unlink(self.db.filename)

    def test_free(self):
        # 0(meta) 1(meta) 2(freelist) 3(leaf)
        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")

        # 0(meta) 1(meta) 2(free) 3(free) 4(leaf) 5(freelist)
        self.assertEqual(self.db.freelist.ids, [2, 3])

        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")

        # 0(meta) 1(meta) 2(leaf) 3(freelist) 4(free) 5(free)
        self.assertEqual(self.db.freelist.ids, [4, 5])

    def test_free2(self):
        # 0(meta) 1(meta) 2(freelist) 3(leaf)
        self.assertEqual(self.db.freepages(), [2])
        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")

        # 0(meta) 1(meta) 2(free) 3(free) 4(leaf) 5(freelist)
        self.assertEqual(sorted(self.db.freepages()), [2, 3, 5])

        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")

        # 0(meta) 1(meta) 2(leaf) 3(freelist) 4(free) 5(free)
        self.assertEqual(sorted(self.db.freepages()), [3, 4, 5])
