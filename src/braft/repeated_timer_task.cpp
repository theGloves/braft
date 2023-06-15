// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Ma,Jingwei(majingwei@baidu.com)

#include "braft/repeated_timer_task.h"
#include "braft/util.h"

namespace braft {

RepeatedTimerTask::RepeatedTimerTask()
    : _timeout_ms(0)
    , _stopped(true)
    , _running(false)
    , _destroyed(true)
    , _invoking(false)
{}

RepeatedTimerTask::~RepeatedTimerTask()
{
    CHECK(!_running) << "Is still running";
    CHECK(_destroyed) << "destroy() must be invoked before descrution";
}

int RepeatedTimerTask::init(int timeout_ms) {
    _timeout_ms = timeout_ms;
    _destroyed = false;
    _stopped = true;
    _running = false;
    _timer = bthread_timer_t();
    return 0;
}

void RepeatedTimerTask::stop() {
    BAIDU_SCOPED_LOCK(_mutex);
    BRAFT_RETURN_IF(_stopped);
    _stopped = true;
    CHECK(_running);
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        _running = false;
        return;
    }
}

void RepeatedTimerTask::on_timedout() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _invoking = true;
    lck.unlock();
    //   ^^^NOTE: don't invoke run() inside lock to avoid the dead-lock issue
    run();
    lck.lock();
    _invoking = false;
    CHECK(_running);
    if (_stopped) {
        _running = false;
        if (_destroyed) {
            // this may call the destruction,
            // so do this after setting _running to false
            lck.unlock();
            on_destroy();
        }
        return;
    }
    // TODO 我的疑问，unique在被析构时会释放锁吗？按理说智能指针是释放资源，
    //  那会不会把整个mutex都析构后续就无法使用
    return schedule(lck);
}

void RepeatedTimerTask::start() {
    // Implementation considers the following conditions:
    //   - The first time start() was invoked
    //   - stop() was not invoked()
    //   - stop() was invoked and _timer was successfully deleted
    //   - stop() was invoked but _timer was not successfully deleted:
    //       a) _timer is still running right now
    //       b) _timer is finished
    std::unique_lock<raft_mutex_t> lck(_mutex);
    BRAFT_RETURN_IF(_destroyed);
    BRAFT_RETURN_IF(!_stopped);
    _stopped = false;

    BRAFT_RETURN_IF(_running);
                 //  ^^^ _timer was not successfully deleted and the former task
                 // is still running, in which case on_timedout would invoke
                 // schedule as it would not see _stopped
    _running = true;
    schedule(lck);
}

void RepeatedTimerTask::run_once_now() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (bthread_timer_del(_timer) == 0) {
        lck.unlock();
        on_timedout(this);
    }
}

void* RepeatedTimerTask::run_on_timedout_in_new_thread(void* arg) {
    RepeatedTimerTask* m = (RepeatedTimerTask*)arg;
    m->on_timedout();
    return NULL;
}

void RepeatedTimerTask::on_timedout(void* arg) {
    // 这里启动一个新的线程去执行on_timeout函数
    // 注释里说不会将timer线程阻塞，如果阻塞会有什么后果？猜测：会影响timer线程的回收让整个Timer类回收变慢，
    // 进而影响节点的退出
    // Start a bthread to invoke run() so we won't block the timer thread.
    // as run() might access the disk so the time it takes is probably beyond
    // expection
    bthread_t tid;
    if (bthread_start_background(
                &tid, NULL, run_on_timedout_in_new_thread, arg) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        run_on_timedout_in_new_thread(arg);
    }
}

// 都是在同一个class内，为什么传锁进来？
// 如果不传，可能会发生：reset和内部on_timedout同时调用schedule。那会有什么危险呢
// 从语义上，schedule是单独抽象出来服用的，应该包含在调用者的临界区中
// 不通过入参传入&锁不支持可重入的话，要求调用者先释放锁再调用schedule，可能有问题
void RepeatedTimerTask::schedule(std::unique_lock<raft_mutex_t>& lck) {
    _next_duetime =
            butil::milliseconds_from_now(adjust_timeout_ms(_timeout_ms));
    if (bthread_timer_add(&_timer, _next_duetime, on_timedout, this) != 0) {
        lck.unlock();
        LOG(ERROR) << "Fail to add timer";
        return on_timedout(this);
    }
}

void RepeatedTimerTask::schedule(){
    std::unique_lock<raft_mutex_t> lck(_mutex);
    lck.lock();
    _next_duetime =
            butil::milliseconds_from_now(adjust_timeout_ms(_timeout_ms));
    if (bthread_timer_add(&_timer, _next_duetime, on_timedout, this) != 0) {
        lck.unlock();
        LOG(ERROR) << "Fail to add timer";
        return on_timedout(this);
    }
    lck.unlock();
}

void RepeatedTimerTask::reset() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    BRAFT_RETURN_IF(_stopped);
    CHECK(_running);
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        return schedule(lck);
    }
    // else on_timedout would invoke schdule
}

void RepeatedTimerTask::reset(int timeout_ms) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _timeout_ms = timeout_ms;
    BRAFT_RETURN_IF(_stopped);
    CHECK(_running);
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        return schedule(lck);
    }
    // else on_timedout would invoke schdule
}

void RepeatedTimerTask::destroy() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    BRAFT_RETURN_IF(_destroyed);
    _destroyed = true;
    if (!_running) {
        CHECK(_stopped);
        lck.unlock();
        on_destroy();
        return;
    }
    BRAFT_RETURN_IF(_stopped);
    _stopped = true;
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        _running = false;
        lck.unlock();
        on_destroy();
        return;
    }
    CHECK(_running);
}

void RepeatedTimerTask::describe(std::ostream& os, bool use_html) {
    (void)use_html;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    const bool stopped = _stopped;
    const bool running = _running;
    const bool destroyed = _destroyed;
    const bool invoking = _invoking;
    const timespec duetime = _next_duetime;
    const int timeout_ms = _timeout_ms;
    lck.unlock();
    os << "timeout(" << timeout_ms << "ms)";
    if (destroyed) {
        os << " DESTROYED";
    }
    if (stopped) {
        os << " STOPPED";
    }
    if (running) {
        if (invoking) {
            os << " INVOKING";
        } else {
            os << " SCHEDULING(in "
               << butil::timespec_to_milliseconds(duetime) - butil::gettimeofday_ms()
               << "ms)";
        }
    }
}

}  //  namespace braft

