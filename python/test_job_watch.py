import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "job", "python"))

from abstraction_job import FileStore, RUNNING, Record, watch
from abstraction_watch import Closed

BUDGET = 0.2


def submit(store, kind="test"):
    r = Record(id="", kind=kind)
    r.spec = {"what": "anything"}
    return store.submit(r)


class Watch(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = FileStore(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_sees_work_that_predates_the_subscription(self):
        jid = submit(self.store)
        sub = watch(self.store, "test")
        self.assertEqual([jid], [r.id for r in sub.records()])
        n = sub.next()
        self.assertEqual(([jid], False), ([r.id for r in n.records], n.quiet))
        sub.close()

    def test_filters_by_kind(self):
        submit(self.store)
        submit(self.store, "something-else")
        sub = watch(self.store, "test")
        self.assertEqual(1, len(sub.records()))
        sub.close()

    def test_quiet_only_when_nothing_visible_moved(self):
        jid = submit(self.store)
        sub = watch(self.store, "test", BUDGET)
        sub.next()
        held = self.store.claim(jid, "owner", 60)
        n = sub.next()
        self.assertEqual((RUNNING, False), (n.records[0].state, n.quiet))
        self.store.renew(jid, held.lease.epoch, 120)
        n = sub.next()
        self.assertTrue(n.quiet and n.silence >= BUDGET, "a renewal is invisible")
        sub.close()

    def test_closed_ends_the_stream(self):
        sub = watch(self.store, "test")
        sub.next()
        threading.Thread(target=sub.close).start()
        with self.assertRaises(Closed):
            sub.next()


if __name__ == "__main__":
    unittest.main()
