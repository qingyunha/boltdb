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
        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")
        self.assertEqual(self.db.freelist.ids, [3])

        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")
        self.assertEqual(self.db.freelist.ids, [4])

    def test_free2(self):
        self.assertEqual(self.db.freepages(), [2])

        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")
        self.assertEqual(sorted(self.db.freepages()), [2, 3])

        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")
        self.assertEqual(sorted(self.db.freepages()), [2, 4])
