#include <abstraction/job/store.h>
#include <abstraction/job/watch.h>
#include <abstraction/watch/watch.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>

namespace fs = std::filesystem;
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

static void a_polled_source_keeps_the_last_good_present_through_an_error() {
    int calls = 0;
    auto s = Sub::poll(
        [&calls]() {
            if (++calls > 1) throw std::runtime_error("share blinked");
            return std::make_pair(std::string("a"), std::string("1"));
        },
        Millis{3600000}, kBudget);
    s->next();
    auto n = s->next();
    check("a polled source keeps the last good present through an error", n && n->quiet && n->now == "a");
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

static fs::path temp_root() {
    const auto stamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    const fs::path root = fs::temp_directory_path() / ("abstraction-watch-test-" + std::to_string(stamp));
    fs::create_directories(root);
    return root;
}

static std::string submit(abstraction::job::FileStore& store, const std::string& kind) {
    abstraction::job::Record r;
    r.kind = kind;
    r.spec = abstraction::job::Json::parse(R"({"what":"anything"})");
    return store.submit(std::move(r));
}

static void a_job_watch_reports_quiet_only_when_nothing_visible_moved() {
    const fs::path root = temp_root();
    abstraction::job::FileStore store(root.string());
    const std::string id = submit(store, "test");
    submit(store, "something-else");
    auto sub = abstraction::job::watch(store, "test", Millis{200});
    check("a job watch sees work that predates it, of its kind only",
          sub.records().size() == 1 && sub.records()[0].id == id);
    sub.next();
    const auto held = store.claim(id, "owner", Millis{60000});
    auto n = sub.next();
    check("a claim is a change", n && !n->quiet && n->records.size() == 1 &&
                                    n->records[0].state == abstraction::job::state::kRunning);
    store.renew(id, held.lease.epoch, Millis{120000});
    auto q = sub.next();
    check("a renewal is invisible, so the next notice is quiet", q && q->quiet && q->silence >= Millis{200});
    sub.close();
    check("a closed job watch says so", !sub.next() && sub.closed());
    fs::remove_all(root);
}

int main() {
    first_notice_is_the_present();
    several_changes_coalesce_into_the_latest();
    the_same_stamp_is_not_a_change();
    quiet_arrives_after_the_budget_and_repeats();
    a_change_ends_the_silence();
    a_polled_source_is_read_before_silence_is_judged();
    a_polled_source_keeps_the_last_good_present_through_an_error();
    closed_ends_a_wait_and_everything_after_it();
    a_timeout_ends_a_wait();
    a_job_watch_reports_quiet_only_when_nothing_visible_moved();
    std::printf("%d failure(s)\n", g_failures);
    return g_failures == 0 ? 0 : 1;
}
