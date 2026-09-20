#include <abstraction/watch/watch.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>

using abstraction::watch::Millis;
using Sub = abstraction::watch::Subscription<std::string>;

static int g_failures = 0;

static void check(const char* name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name);
    if (!ok) ++g_failures;
}

constexpr Millis kBudget{40};

static void first_notice_is_the_present() {
    auto s = Sub::push("a", "1", kBudget);
    auto n = s->next();
    check("first notice is the present", n && n->now == "a" && !n->quiet);
}

static void several_changes_coalesce_into_the_latest() {
    auto s = Sub::push("a", "1", kBudget);
    s->next();
    s->post("b", "2");
    s->post("c", "3");
    auto n = s->next();
    check("several changes coalesce into the latest", n && n->now == "c" && !n->quiet);
    auto q = s->next();
    check("no backlog was kept", q && q->quiet);
}

static void the_same_stamp_is_not_a_change() {
    auto s = Sub::push("a", "1", kBudget);
    s->next();
    check("the same stamp is not a change", !s->post("a", "1"));
    auto q = s->next();
    check("the same present twice reads as quiet", q && q->quiet);
}

static void quiet_arrives_after_the_budget_and_repeats() {
    auto s = Sub::push("a", "1", kBudget);
    s->next();
    auto first = s->next();
    auto second = s->next();
    check("quiet arrives after the budget", first && first->quiet && first->silence >= kBudget);
    check("quiet repeats one budget later", second && second->quiet && second->silence >= 2 * kBudget);
}

static void a_change_ends_the_silence() {
    auto s = Sub::push("a", "1", kBudget);
    s->next();
    s->next();
    s->post("b", "2");
    auto n = s->next();
    check("a change ends the silence", n && n->now == "b" && !n->quiet);
    auto q = s->next();
    check("the next silence is measured from the change",
          q && q->quiet && q->silence >= kBudget && q->silence <= 5 * kBudget);
}

static void a_polled_source_is_read_before_silence_is_judged() {
    int calls = 0;
    auto s = Sub::poll(
        [&calls]() {
            ++calls;
            return calls >= 3 ? std::make_pair(std::string("moved"), std::string("2"))
                              : std::make_pair(std::string("still"), std::string("1"));
        },
        Millis{3600000}, kBudget);
    s->next();
    auto n = s->next();
    check("a polled source is read before silence is judged", n && n->now == "moved" && !n->quiet);
}

static void a_failing_polled_source_is_unavailable_and_never_quiet() {
    std::ifstream fixture(std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() / "go/testdata/poll-outage.txt");
    int budget_ms, poll_ms, outage_ms, outages, expected;
    if (!(fixture >> budget_ms >> poll_ms >> outage_ms >> outages >> expected)) {
        check("outage fixture readable", false);
        return;
    }
    std::atomic<bool> failing{false};
    auto s = Sub::poll(
        [&failing]() {
            if (failing) throw std::runtime_error("share blinked");
            return std::make_pair(std::string("a"), std::string("1"));
        },
        Millis{poll_ms}, Millis{budget_ms});
    s->next();
    for (int outage = 0; outage < outages; ++outage) {
        failing = true;
        const auto deadline = std::chrono::steady_clock::now() + Millis{outage_ms};
        int unavailable = 0;
        bool unexpected = false;
        for (;;) {
            const auto left = std::chrono::duration_cast<Millis>(deadline - std::chrono::steady_clock::now());
            if (left.count() <= 0) break;
            auto n = s->next(left);
            if (!n) break;
            if (n->quiet || !n->unavailable || n->now != "a") unexpected = true;
            ++unavailable;
        }
        check("a failing polled source is unavailable once and never quiet", unavailable == expected && !unexpected);
        failing = false;
        auto n = s->next();
        check("a read with the same stamp clears unavailable", n && !n->unavailable && !n->quiet && n->now == "a");
    }
}

static void closed_ends_a_wait_and_everything_after_it() {
    auto s = Sub::push("a", "1", Millis{0});
    s->next();
    std::thread closer([&s]() { s->close(); });
    auto n = s->next();
    closer.join();
    check("closed ends a wait", !n && s->closed());
    s->post("b", "2");
    check("nothing follows closed", !s->next());
}

static void a_timeout_ends_a_wait() {
    auto s = Sub::push("a", "1", Millis{0});
    s->next();
    auto n = s->next(kBudget);
    check("a timeout ends a wait without closing", !n && !s->closed());
}

int main() {
    first_notice_is_the_present();
    several_changes_coalesce_into_the_latest();
    the_same_stamp_is_not_a_change();
    quiet_arrives_after_the_budget_and_repeats();
    a_change_ends_the_silence();
    a_polled_source_is_read_before_silence_is_judged();
    a_failing_polled_source_is_unavailable_and_never_quiet();
    closed_ends_a_wait_and_everything_after_it();
    a_timeout_ends_a_wait();
    std::printf("%d failure(s)\n", g_failures);
    return g_failures == 0 ? 0 : 1;
}
