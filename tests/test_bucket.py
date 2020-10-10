import os
import unittest
import tempfile

from boltdb import BoltDB


class TestBucket(unittest.TestCase):

    def setUp(self):
        self.db = BoltDB(tempfile.mktemp())

    def tearDown(self):
        os.unlink(self.db.filename)

    def test_get_nonexistent(self):
        with self.db.update() as tx:
            b = tx.bucket()
            v = b.get(b"foo")
            self.assertIsNone(v)

    def test_get_from_node(self):
        with self.db.update() as tx:
            b = tx.bucket()
            b.put(b"foo", b"bar")
            v = b.get(b"foo")
            self.assertEqual(v, b"bar")

    def test_get_bucket_is_none(self):
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            tx.bucket(b"widgets").create_bucket(b"foo")
            self.assertIsNone(tx.bucket(b"widgets").get(b"foo"))

    def test_put(self):
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            b.put(b"foo", b"bar")
            v = tx.bucket(b"widgets").get(b"foo")
            self.assertEqual(v, b"bar")

    def test_put_repeat(self):
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            b.put(b"foo", b"bar")
            b.put(b"foo", b"baz")
            v = tx.bucket(b"widgets").get(b"foo")
            self.assertEqual(v, b"baz")

    def test_put_large(self):
        count, factor = 100, 200
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            for i in range(1, count):
                b.put(b"0"*i*factor, b"X"*(count-1)*factor)

        with self.db.view() as tx:
            b = tx.bucket(b"widgets")
            for i in range(1, count):
                v = b.get(b"0"*i*factor)
                self.assertEqual(v, b"X"*(count-1)*factor)

    def test_put_incompatible(self):
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            tx.bucket(b"widgets").create_bucket(b"foo")
            with self.assertRaisesRegex(Exception, "cannot write sub bucket"):
                b.put(b"foo", b"bar")

    def test_iter(self):
        orderd_tyes = b"abcdefghijklmnopqrstuvwxyz"
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            for i in range(len(orderd_tyes)):
                b.put(orderd_tyes[i:], b"foo")

        with self.db.view() as tx:
            b = tx.bucket(b"widgets")
            for i, (k, _) in enumerate(b):
                self.assertEqual(k, orderd_tyes[i:])
            self.assertEqual(i, len(orderd_tyes)-1)

    def test_delete(self):
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            b.put(b"foo", b"bar")
            b.delete(b"foo")
            v = tx.bucket(b"widgets").get(b"foo")
            self.assertIsNone(v)

    def test_delete_large(self):
        count = 100
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            for i in range(count):
                b.put(str(i).encode(), b"*" * 1024)

        with self.db.update() as tx:
            b = tx.bucket(b"widgets")
            for i in range(count):
                b.delete(str(i).encode())

        with self.db.view() as tx:
            b = tx.bucket(b"widgets")
            for i in range(count):
                v = b.get(str(i).encode())
                self.assertIsNone(v)

    def test_delete_nonexisting(self):
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            b.create_bucket(b"nested")
        with self.db.update() as tx:
            b = tx.bucket(b"widgets")
            b.delete(b"foo")
            self.assertIsNotNone(b.bucket(b"nested"))

    def test_nested(self):
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            b.create_bucket(b"foo")
            b.put(b"bar", b"0000")

        # Update widgets/bar.
        with self.db.update() as tx:
            b = tx.bucket(b"widgets")
            b.put(b"bar", b"xxxx")

        # Cause a split.
        with self.db.update() as tx:
            b = tx.bucket(b"widgets")
            for i in range(10000):
                k = str(i).encode()
                b.put(k, k)

        # Insert into widgets/foo/baz.
        with self.db.update() as tx:
            b = tx.bucket(b"widgets").bucket(b"foo")
            b.put(b"baz", b"yyyy")

        with self.db.view() as tx:
            b = tx.bucket(b"widgets")
            v = b.bucket(b"foo").get(b"baz")
            self.assertEqual(v, b"yyyy")
            v = b.get(b"bar")
            self.assertEqual(v, b"xxxx")
            for i in range(10000):
                k = str(i).encode()
                v = b.get(k)
                self.assertEqual(v, k)

    def test_delete_a_bucket(self):
        # Ensure that deleting a bucket using delete() returns an error.
        with self.db.update() as tx:
            b = tx.create_bucket(b"widgets")
            b.create_bucket(b"foo")
            with self.assertRaises(Exception):
                b.delete(b"foo")

    def test_delete_bucket_nested(self):
        with self.db.update() as tx:
            widgets = tx.create_bucket(b"widgets")
            foo = widgets.create_bucket(b"foo")
            bar = foo.create_bucket(b"bar")
            bar.put(b"baz", b"bat")
            widgets.delete_bucket(b"foo")
            self.assertIsNone(widgets.bucket(b"foo"))

        with self.db.view() as tx:
            widgets = tx.bucket(b"widgets")
            self.assertIsNone(widgets.bucket(b"foo"))

    def test_delete_bucket_large(self):
        with self.db.update() as tx:
            widgets = tx.create_bucket(b"widgets")
            foo = widgets.create_bucket(b"foo")
            for i in range(1000):
                k = str(i).encode()
                foo.put(k, k)

        with self.db.update() as tx:
            widgets = tx.delete_bucket(b"widgets")

        with self.db.update() as tx:
            widgets = tx.create_bucket(b"widgets")
            self.assertIsNone(widgets.bucket(b"foo"))

    def test_create_bucket_incompatible(self):
        with self.db.update() as tx:
            widgets = tx.create_bucket(b"widgets")
            widgets.put(b"foo", b"bar")
            with self.assertRaisesRegex(Exception, "incompatible value"):
                widgets.create_bucket(b"foo")

    def test_delete_bucket_incompatible(self):
        with self.db.update() as tx:
            widgets = tx.create_bucket(b"widgets")
            widgets.put(b"foo", b"bar")
            with self.assertRaisesRegex(Exception, "bucket not exists"):
                widgets.delete_bucket(b"foo")
