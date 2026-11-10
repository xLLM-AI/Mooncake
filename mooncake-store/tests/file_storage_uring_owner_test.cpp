#include <gtest/gtest.h>
#include <liburing.h>
#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "file_interface.h"
#include "file_storage.h"

namespace {
// Model buffer tables only; queue setup, submissions and byte reads are real
// io_uring. This exercises ownership on kernels without clone support, without
// claiming a native clone/pin test or changing the production implementation.
struct BufferTables {
    std::mutex mutex;
    std::unordered_set<io_uring*> live;
    size_t registrations = 0;
    size_t unregistrations = 0;
};
BufferTables& tables() {
    static auto* state = new BufferTables;
    return *state;
}
thread_local std::function<void(bool)>* before_submit = nullptr;
}  // namespace

extern "C" int __wrap_io_uring_register_buffers(io_uring* ring, const iovec*,
                                                unsigned) {
    std::lock_guard lock(tables().mutex);
    tables().live.insert(ring);
    ++tables().registrations;
    return 0;
}
extern "C" int __wrap_io_uring_clone_buffers(io_uring* dst, io_uring* src) {
    std::lock_guard lock(tables().mutex);
    if (!tables().live.count(src)) return -ENOENT;
    tables().live.insert(dst);
    return 0;
}
extern "C" int __wrap_io_uring_unregister_buffers(io_uring* ring) {
    std::lock_guard lock(tables().mutex);
    tables().live.erase(ring);
    ++tables().unregistrations;
    return 0;
}
extern "C" int __real_io_uring_submit_and_wait(io_uring*, unsigned);
extern "C" int __wrap_io_uring_submit_and_wait(io_uring* ring, unsigned count) {
    bool fixed = false;
    for (unsigned i = ring->sq.sqe_head; i != ring->sq.sqe_tail; ++i) {
        const auto& sqe = ring->sq.sqes[i & ring->sq.ring_mask];
        fixed |= sqe.opcode == IORING_OP_READ_FIXED;
    }
    // Pause after the production code has selected/prepared its SQE, but
    // before submission checks the modeled table. Never hold a lock here.
    auto* hook = before_submit;
    before_submit = nullptr;
    if (hook) (*hook)(fixed);
    {
        std::lock_guard lock(tables().mutex);
        if (fixed && !tables().live.count(ring)) return -EFAULT;
    }
    // The modeled table is not a native kernel table: perform the same byte
    // read using ordinary io_uring after asserting fixed-index validity above.
    for (unsigned i = ring->sq.sqe_head; i != ring->sq.sqe_tail; ++i) {
        auto& sqe = ring->sq.sqes[i & ring->sq.ring_mask];
        if (sqe.opcode == IORING_OP_READ_FIXED) {
            sqe.opcode = IORING_OP_READ;
            sqe.buf_index = 0;
        }
    }
    return __real_io_uring_submit_and_wait(ring, count);
}

namespace mooncake {
// Reuse FileStorage's existing tests-only fixture friendship, without adding
// a production friend, access-control macro or cast to a fake derived type.
class FileStorageTest : public ::testing::Test {
   protected:
    void SetUp() override {
#if defined(IO_URING_VERSION_MAJOR) && \
    (IO_URING_VERSION_MAJOR > 2 ||     \
     (IO_URING_VERSION_MAJOR == 2 && IO_URING_VERSION_MINOR >= 9))
        io_uring ring{};
        if (io_uring_queue_init(2, &ring, 0) < 0)
            GTEST_SKIP() << "io_uring is unavailable";
        io_uring_queue_exit(&ring);
#else
        GTEST_SKIP() << "liburing SDK lacks the clone API for this model";
#endif
        std::array<char, 64> name{};
        std::strcpy(name.data(), "/tmp/mooncake-owner-XXXXXX");
        ASSERT_NE(mkdtemp(name.data()), nullptr);
        directory_ = name.data();
    }
    void TearDown() override {
        if (!directory_.empty()) std::filesystem::remove_all(directory_);
    }
    std::unique_ptr<FileStorage> storage(const char* name) {
        FileStorageConfig config;
        config.storage_filepath = directory_ + "/" + name;
        config.local_buffer_size = 64 * 1024;
        config.use_uring = true;
        std::filesystem::create_directories(config.storage_filepath);
        return std::make_unique<FileStorage>(config, nullptr, name);
    }
    static char* buffer(FileStorage& storage) {
        return static_cast<char*>(storage.client_buffer_allocator_->getBase());
    }
    static std::pair<size_t, size_t> calls() {
        std::lock_guard lock(tables().mutex);
        return {tables().registrations, tables().unregistrations};
    }
    void read_while_destroying_other(bool destroy_owner) {
        auto a = storage("A");
        auto b = storage("B");
        FileStorage& reader_storage = destroy_owner ? *b : *a;
        auto& retiring = destroy_owner ? a : b;
        auto* dst = buffer(reader_storage);
        const std::string file = directory_ + "/data";
        int fd = open(file.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
        ASSERT_GE(fd, 0);
        std::array<char, 4096> expected{};
        expected.fill('b');
        ASSERT_EQ(pwrite(fd, expected.data(), expected.size(), 0),
                  static_cast<ssize_t>(expected.size()));
        std::promise<void> checked, resume;
        auto entered = checked.get_future();
        auto released = resume.get_future().share();
        bool selected_fixed = false, read_ok = false;
        // This callback outlives the reader, including TLS ring destruction.
        std::function<void(bool)> gate = [&](bool fixed) {
            selected_fixed = fixed;
            checked.set_value();
            released.wait();
        };
        std::thread reader([&] {
            UringFile input(file, fd, 32, false);
            before_submit = &gate;
            UringFile::ReadDesc desc{dst, expected.size(), 0};
            read_ok = input.batch_read(&desc, 1).has_value() &&
                      desc.bytes_read == expected.size() &&
                      std::memcmp(dst, expected.data(), expected.size()) == 0;
            std::memset(dst, 0, expected.size());
            UringFile::ReadDesc next{dst, expected.size(), 0};
            read_ok = read_ok && input.batch_read(&next, 1).has_value() &&
                      next.bytes_read == expected.size();
        });
        const auto ready = entered.wait_for(std::chrono::seconds(5));
        const auto before = calls();
        if (ready == std::future_status::ready) {
            retiring.reset();
            // Check before the reader exits: its normal TLS ring cleanup is
            // allowed to drop its own clone, unrelated to storage destruction.
            if (!destroy_owner) EXPECT_EQ(calls().second, before.second);
        }
        resume.set_value();
        reader.join();
        ASSERT_EQ(ready, std::future_status::ready);
        EXPECT_EQ(selected_fixed, !destroy_owner);
        ASSERT_TRUE(read_ok);
        EXPECT_EQ(std::memcmp(dst, expected.data(), expected.size()), 0);
    }
    std::string directory_;
};

TEST_F(FileStorageTest, AdditionalInstanceDoesNotReplaceActiveRegistration) {
    auto a = storage("A");
    const auto before = calls();
    auto b = storage("B");
    EXPECT_EQ(calls(), before);
}

TEST_F(FileStorageTest, FallbackReadSurvivesRegisteredOwnerDestruction) {
    read_while_destroying_other(true);
}

TEST_F(FileStorageTest, FixedReadSurvivesFallbackInstanceDestruction) {
    read_while_destroying_other(false);
}
TEST_F(FileStorageTest, StaleTokenCannotReleaseReusedAddress) {
    auto memory = std::unique_ptr<void, decltype(&std::free)>(
        std::aligned_alloc(4096, 64 * 1024), &std::free);
    ASSERT_NE(memory, nullptr);
    const auto old = UringFile::acquire_global_buffer(memory.get(), 64 * 1024);
    ASSERT_NE(old, 0);
    UringFile::release_global_buffer(old);
    const auto current =
        UringFile::acquire_global_buffer(memory.get(), 64 * 1024);
    ASSERT_NE(current, 0);
    EXPECT_NE(current, old);
    const auto before = calls();
    UringFile::release_global_buffer(old);
    UringFile::unregister_global_buffer();
    EXPECT_EQ(calls(), before);
    EXPECT_FALSE(UringFile::register_global_buffer(memory.get(), 64 * 1024));
    EXPECT_EQ(calls(), before);
    UringFile::release_global_buffer(current);
    EXPECT_GT(calls().second, before.second);
}
}  // namespace mooncake
