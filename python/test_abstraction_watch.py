import queue
import threading
import unittest

from abstraction_watch import Closed, poll, push, settle

BUDGET = 0.04


class Push(unittest.TestCase):
    def test_first_notice_is_the_present(self):
        s = push("a", "1", BUDGET)
        n = s.next()
        self.assertEqual(("a", False), (n.now, n.quiet))

    def test_several_changes_coalesce_into_the_latest(self):
        s = push("a", "1", BUDGET)
        s.next()
        s.post("b", "2")
        s.post("c", "3")
        self.assertEqual("c", s.next().now)
        self.assertTrue(s.next().quiet, "a backlog was kept")

    def test_the_same_stamp_is_not_a_change(self):
        s = push("a", "1", BUDGET)
        s.next()
        self.assertFalse(s.post("a", "1"))
        self.assertTrue(s.next().quiet)

    def test_quiet_arrives_after_the_budget_and_repeats(self):
        s = push("a", "1", BUDGET)
        s.next()
        first, second = s.next(), s.next()
        self.assertTrue(first.quiet and first.silence >= BUDGET)
        self.assertTrue(second.quiet and second.silence >= 2 * BUDGET)

    def test_a_change_ends_the_silence(self):
        s = push("a", "1", BUDGET)
        s.next()
        s.next()
        s.post("b", "2")
        n = s.next()
        self.assertEqual(("b", False), (n.now, n.quiet))
        n = s.next()
        self.assertTrue(n.quiet and BUDGET <= n.silence <= 5 * BUDGET)

    def test_closed_ends_a_wait_and_everything_after_it(self):
        s = push("a", "1", 0.0)
        s.next()
        threading.Thread(target=s.close).start()
        with self.assertRaises(Closed):
            s.next()
        s.post("b", "2")
        with self.assertRaises(Closed):
            s.next()

    def test_a_timeout_ends_a_wait(self):
        s = push("a", "1", 0.0)
        s.next()
        with self.assertRaises(TimeoutError):
            s.next(timeout=BUDGET)

    def test_iteration_ends_on_close(self):
        s = push("a", "1", 0.0)
        s.close()
        self.assertEqual([], list(s))


class Poll(unittest.TestCase):
    def test_a_polled_source_is_read_before_silence_is_judged(self):
        calls = [0]

        def read():
            calls[0] += 1
            return ("moved", "2") if calls[0] >= 3 else ("still", "1")

        s = poll(read, 3600.0, BUDGET)
        s.next()
        n = s.next()
        self.assertEqual(("moved", False), (n.now, n.quiet))

    def test_a_polled_source_keeps_the_last_good_present_through_an_error(self):
        calls = [0]

        def read():
            calls[0] += 1
            if calls[0] > 1:
                raise OSError("share blinked")
            return ("a", "1")

        s = poll(read, 3600.0, BUDGET)
        s.next()
        n = s.next()
        self.assertEqual(("a", True), (n.now, n.quiet))

    def test_two_subscriptions_are_independent(self):
        read = lambda: (1, "1")
        a, b = poll(read, 3600.0, BUDGET), poll(read, 3600.0, BUDGET)
        a.next()
        self.assertFalse(b.next().quiet, "taking a's present took b's")


class Settle(unittest.TestCase):
    def run_settle(self, q):
        rang = threading.Semaphore(0)
        t = threading.Thread(target=settle, args=(q, BUDGET, rang.release),
                             daemon=True)
        t.start()
        return rang, t

    def test_a_burst_produces_one_call(self):
        q = queue.Queue()
        rang, t = self.run_settle(q)
        for _ in range(5):
            q.put(object())
        self.assertTrue(rang.acquire(timeout=5), "the burst never settled")
        self.assertFalse(rang.acquire(timeout=4 * BUDGET),
                         "five signals produced more than one settled read")
        q.put(None)
        t.join(timeout=5)

    def test_a_still_source_calls_nothing(self):
        q = queue.Queue()
        rang, t = self.run_settle(q)
        self.assertFalse(rang.acquire(timeout=4 * BUDGET),
                         "nothing moved and it read anyway")
        q.put(None)
        t.join(timeout=5)

    def test_none_ends_it(self):
        q = queue.Queue()
        _, t = self.run_settle(q)
        q.put(None)
        t.join(timeout=5)
        self.assertFalse(t.is_alive(), "None on the queue did not stop it")

    def test_a_second_burst_is_reported_too(self):
        q = queue.Queue()
        rang, t = self.run_settle(q)
        q.put(object())
        self.assertTrue(rang.acquire(timeout=5))
        q.put(object())
        self.assertTrue(rang.acquire(timeout=5), "it settled once and stopped")
        q.put(None)
        t.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
