#pragma once

// watch -- the present, and whether it has stopped changing.
//
// A Notice carries what is true now. `quiet` says nothing visible has changed
// for at least the budget, and `silence` says for how long by this observer's
// clock. The listener's own wait is the only timer: quiet is judged by a
// listener that is waiting, after it has read, so a change the source already
// made is always delivered ahead of a quiet that would postdate it.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace abstraction::watch {

using Millis = std::chrono::milliseconds;

constexpr Millis kDefaultEvery{1000};

template <class T>
struct Notice {
    T now;
    bool quiet = false;
    Millis silence{0};
};

template <class T>
class Subscription {
public:
    using Source = std::function<std::pair<T, std::string>()>;
    using Clock = std::chrono::steady_clock;

    // A source that has to be asked: read on entry to next and then every
    // `every` while a listener waits.
    static std::unique_ptr<Subscription> poll(Source read, Millis every, Millis budget) {
        std::unique_ptr<Subscription> s(new Subscription(std::move(read), every.count() > 0 ? every : kDefaultEvery, budget));
        s->refresh();
        s->pending_ = true;
        return s;
    }

    // A source that says when it moved, through post.
    static std::unique_ptr<Subscription> push(T first, std::string stamp, Millis budget) {
        std::unique_ptr<Subscription> s(new Subscription(nullptr, Millis{0}, budget));
        s->now_ = std::move(first);
        s->stamp_ = std::move(stamp);
        s->pending_ = true;
        return s;
    }

    bool post(T value, std::string stamp) {
        std::lock_guard<std::mutex> lk(mu_);
        const bool changed = take(std::move(value), std::move(stamp));
        if (changed) cv_.notify_all();
        return changed;
    }

    T current() const {
        std::lock_guard<std::mutex> lk(mu_);
        return now_;
    }

    // Something to say, or nullopt: closed, or the timeout passed with nothing.
    std::optional<Notice<T>> next(std::optional<Millis> timeout = std::nullopt) {
        std::optional<Clock::time_point> deadline;
        if (timeout) deadline = Clock::now() + *timeout;
        for (;;) {
            refresh();
            std::unique_lock<std::mutex> lk(mu_);
            if (closed_) return std::nullopt;
            if (pending_) {
                pending_ = false;
                return Notice<T>{now_, false, Millis{0}};
            }
            const auto now = Clock::now();
            std::optional<Clock::time_point> until;
            if (read_) until = now + every_;
            if (budget_.count() > 0) {
                const auto since = std::max(changed_, told_);
                if (now - since >= budget_) {
                    told_ = now;
                    return Notice<T>{now_, true, std::chrono::duration_cast<Millis>(now - changed_)};
                }
                if (!until || since + budget_ < *until) until = since + budget_;
            }
            if (deadline) {
                if (now >= *deadline) return std::nullopt;
                if (!until || *deadline < *until) until = *deadline;
            }
            if (until) {
                cv_.wait_until(lk, *until);
            } else {
                cv_.wait(lk);
            }
        }
    }

    bool closed() const {
        std::lock_guard<std::mutex> lk(mu_);
        return closed_;
    }

    void close() {
        std::lock_guard<std::mutex> lk(mu_);
        closed_ = true;
        cv_.notify_all();
    }

private:
    Subscription(Source read, Millis every, Millis budget)
        : read_(std::move(read)), every_(every), budget_(budget), changed_(Clock::now()) {}

    bool take(T value, std::string stamp) {
        if (stamp == stamp_) return false;
        now_ = std::move(value);
        stamp_ = std::move(stamp);
        pending_ = true;
        changed_ = Clock::now();
        return true;
    }

    // A source that cannot be read right now is not an empty one.
    void refresh() {
        if (!read_) return;
        std::pair<T, std::string> got;
        try {
            got = read_();
        } catch (...) {
            return;
        }
        std::lock_guard<std::mutex> lk(mu_);
        take(std::move(got.first), std::move(got.second));
    }

    Source read_;
    Millis every_;
    Millis budget_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    T now_{};
    std::string stamp_;
    bool pending_ = false;
    Clock::time_point changed_;
    Clock::time_point told_{};
    bool closed_ = false;
};

}  // namespace abstraction::watch
