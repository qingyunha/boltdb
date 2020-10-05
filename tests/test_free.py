import os
import unittest
import tempfile

from pybolt import BoltDB


class TestFree(unittest.TestCase):

    def setUp(self):
        self.db = BoltDB(tempfile.mktemp())

    def tearDown(self):
        os.unlink(self.db.filename)

    def test_free(self):
        with self.db.update() as tx:
            b = tx.bucket()
            v = b.put(b"foo", b"bar")
        self.assertEqual(self.db.freelist.ids, [3])

        with self.db.update() as tx:
            b = tx.bucket()
            v = b.put(b"foo", b"bar")
        self.assertEqual(self.db.freelist.ids, [4])
