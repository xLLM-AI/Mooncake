// =============================================================================
// 【草案 v3 - 待审阅,尚未加入编译】client 驱动的元数据重建 单测
//
// 目标:验证新方案——master 挂掉重启后,client 把持有的 key→location 元数据
//       重发给新 master,重建完整元数据,实现零重算恢复。
//
// 6 个测试(覆盖矩阵见权威文档 §11.3.1):
//   测试1 RebuildObjectMetadataAfterMasterRestart —— 核心重建(单client自记账,步骤1-3)
//   测试2 RebuiltMetadataPointsToRealData         —— 防假恢复,逐字节比对(单client)
//   测试3 LazyDelete_RemovedButNotReused_MayRevive —— 惰性删语义:删了未复用可复活(数据仍对)
//   测试4 CrossClientRebuildViaNotify             —— 【多client·方案核心】跨client notify+B重建
//   测试5 RemovedKeySpaceReuseNoStaleMapping      —— 【惰性删核心正确性】复用后旧key被地址覆盖,不复活
//   测试6 MultiReplicaMergedOnRebuild             —— 【多副本合并】replica_num=2,重建后副本数恢复==2
//
// ⚠️ 分工:测试1/2/3/5 是【单 client】,测"记账/删除/重建/复用防护"这些零件本身;
//   测试4 是【多 client】,测本方案的核心——A 数据落 B 段、靠 notify 让 B 记账、
//   master 重启后 B 重建。你的新程序是多 client 的,测试4 才是主力验证。
//   测试1/2/3 全绿 ≠ 方案完全正确(它们不触发 notify);测试4 才覆盖 notify 核心路径。
//
// ⚠️ 前提:这些测试要真正通过,依赖新方案代码已实现(见实现文档):
//     - client:local_replica_table_ 成员 + Put/BatchPut 记账 + Remove 清理
//               + 重连重发 RebuildMetadata + (跨段场景) notify 收发。
//     - master:RebuildMetadata RPC + DescriptorToReplica + 落库。
//   方案实现前,测试会因新 master 返回 OBJECT_NOT_FOUND 而失败(预期的 TDD "红")。
//
// ⚠️ v2 相对 v1 的修正(都是照 v1 会编译不过/行为错的真实问题):
//   1. 用 SimpleAllocator(allocate 返回 void*),不是 ClientBufferAllocator
//      (后者 allocate 返回 std::optional<BufferHandle>,签名对不上)。
//   2. 数据缓冲区必须先 RegisterLocalMemory,否则 Put 无法用本地 buffer 传输。
//   3. MountSegment 用三参重载(带 protocol)。
//   4. Remove 必须传 force=true —— 否则受 lease 阻挡返回 OBJECT_HAS_LEASE
//      (master_service.cpp:4497:if(!force && !IsLeaseExpired()) return OBJECT_HAS_LEASE)。
//
// ⚠️ v3 相对 v2 的修正(审查发现):
//   5. 探针改为 WaitForAllKeysRebuilt(等【全部】key 重建)而非单键探针——
//      避免 RebuildMetadata 逐键/增量实现下"探针键先到、末尾键未到即断言"的假失败。
//   6. 新增测试5(地址复用覆盖)——惰性删的核心正确性(复用后旧 key 被覆盖不复活)。
//
// ⚠️ v4(惰性删语义定稿):Remove 时 client 本地表【不删】。测试3 改为验证"删了但空间
//    未复用的 key 重建时【允许复活】且数据仍正确"(惰性删预期,非 bug);故测试3 也等
//    【全部】key(含被删的,它们会复活)。"复用后旧 key 不复活"由测试5 保证。
//
// 正式加入:文件移到 mooncake-store/tests/client_metadata_rebuild_test.cpp,
//          tests/CMakeLists.txt 加:
//          add_store_test(client_metadata_rebuild_test client_metadata_rebuild_test.cpp)
// =============================================================================

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "types.h"
#include "utils.h"                 // allocate_buffer_allocator_memory, SimpleAllocator
#include "test_server_helpers.h"   // InProcMaster, InProcMasterConfigBuilder
#include "default_config.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");

namespace mooncake {
namespace testing {

namespace {

// ⚠️ 关键:等待"全部 key"都能 Get 到,而不是只探一个 key。
// 原因(审查发现的实质缺陷):若 RebuildMetadata 是逐键/增量推送(非整表原子落库),
// 探针 key 先到而 keys[N-1] 未到时,立即全量断言会误判"存活键丢失"。
// 这里以"全部 key 都 Get 成功"作为重建完成判据,兼容增量与原子两种实现。
// 返回 false 表示超时仍有 key 未重建。
bool WaitForAllKeysRebuilt(std::shared_ptr<Client>& client,
                           SimpleAllocator& allocator,
                           const std::vector<std::string>& keys,
                           const std::vector<std::string>& values,
                           int max_attempts = 40, int interval_ms = 500) {
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        bool all_ok = true;
        for (size_t i = 0; i < keys.size(); ++i) {
            void* buf = allocator.allocate(values[i].size());
            std::vector<Slice> slices{Slice{buf, values[i].size()}};
            auto res = client->Get(keys[i], slices);
            allocator.deallocate(buf, values[i].size());
            if (!res.has_value()) { all_ok = false; break; }
        }
        if (all_ok) {
            LOG(INFO) << "All " << keys.size() << " keys rebuilt after "
                      << attempt << " polls";
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
    return false;
}

tl::expected<void, ErrorCode> PutString(std::shared_ptr<Client>& client,
                                        SimpleAllocator& allocator,
                                        const std::string& key,
                                        const std::string& value) {
    void* buf = allocator.allocate(value.size());
    std::memcpy(buf, value.data(), value.size());
    std::vector<Slice> slices{Slice{buf, value.size()}};
    ReplicateConfig config;
    config.replica_num = 1;
    auto res = client->Put(key, slices, config);
    allocator.deallocate(buf, value.size());
    return res;
}

// Get 并逐字节比对取回内容与期望是否一致。
bool GetAndVerify(std::shared_ptr<Client>& client, SimpleAllocator& allocator,
                  const std::string& key, const std::string& expected) {
    void* buf = allocator.allocate(expected.size());
    std::vector<Slice> slices{Slice{buf, expected.size()}};
    auto res = client->Get(key, slices);
    bool ok = res.has_value() && slices[0].size == expected.size() &&
              std::memcmp(slices[0].ptr, expected.data(), expected.size()) == 0;
    allocator.deallocate(buf, expected.size());
    return ok;
}

}  // namespace

class ClientMetadataRebuildTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // 进程内 non-HA master(自动选端口)。
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        master_address_ = master_.master_address();

        // 建 client。
        local_hostname_ = "127.0.0.1:19100";
        auto client_opt = Client::Create(local_hostname_, "P2PHANDSHAKE",
                                          FLAGS_protocol, std::nullopt,
                                          master_address_);
        ASSERT_TRUE(client_opt.has_value()) << "Failed to create client";
        client_ = client_opt.value();

        // 数据缓冲区分配器 + 注册为本地内存(修正2:Put 传输前必须注册)。
        allocator_ = std::make_unique<SimpleAllocator>(kAllocSize);
        auto reg = client_->RegisterLocalMemory(
            allocator_->getBase(), kAllocSize, "cpu:0", false, false);
        ASSERT_TRUE(reg.has_value()) << "RegisterLocalMemory failed";

        // 挂一块段(数据落脚处),修正3:三参重载带 protocol。
        seg_ptr_ = allocate_buffer_allocator_memory(kSegmentSize);
        ASSERT_NE(seg_ptr_, nullptr);
        auto mount = client_->MountSegment(seg_ptr_, kSegmentSize, FLAGS_protocol);
        ASSERT_TRUE(mount.has_value()) << toString(mount.error());
    }

    void TearDown() override {
        if (client_ && seg_ptr_) {
            client_->UnmountSegment(seg_ptr_, kSegmentSize);
        }
        master_.Stop();
    }

    // 模拟主 master 故障 → 空状态新 master(同端口,client 才能重连回来)。
    void RestartMasterEmpty() {
        master_.Stop();
        std::this_thread::sleep_for(std::chrono::seconds(3));  // 等心跳失败累积
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_rpc_port(master_.rpc_port())
                                      .set_http_metrics_port(
                                          master_.http_metrics_port())
                                      .build()));
    }

    static constexpr size_t kSegmentSize = 128 * 1024 * 1024;   // 128MB 段
    static constexpr size_t kAllocSize = 64 * 1024 * 1024;      // 64MB 缓冲区

    InProcMaster master_;
    std::string master_address_;
    std::string local_hostname_;
    std::shared_ptr<Client> client_;
    void* seg_ptr_ = nullptr;
    std::unique_ptr<SimpleAllocator> allocator_;
};

// ---------------------------------------------------------------------------
// 测试1(核心):master 重启后 client 重建对象元数据,数据零重算可读
// ---------------------------------------------------------------------------
TEST_F(ClientMetadataRebuildTest, RebuildObjectMetadataAfterMasterRestart) {
    const int kNumKeys = 50;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("rebuild_key_" + std::to_string(i));
        values.push_back("rebuild_value_" + std::to_string(i));
    }
    for (int i = 0; i < kNumKeys; ++i) {
        auto r = PutString(client_, *allocator_, keys[i], values[i]);
        ASSERT_TRUE(r.has_value())
            << "Put failed " << keys[i] << ": " << toString(r.error());
    }
    // 基线:重启前全部可读回。
    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_TRUE(GetAndVerify(client_, *allocator_, keys[i], values[i]))
            << "Baseline Get failed " << keys[i];

    RestartMasterEmpty();

    // 等待【全部】key 重建(不是只探一个,避免增量实现下的假失败)。
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, values))
        << "元数据未在窗口内全部重建:新 master 仍 NOT_FOUND,或重发/重建链路未生效";

    // 核心断言:重建后每个 key 可读且内容一致(零重算)。
    for (int i = 0; i < kNumKeys; ++i)
        EXPECT_TRUE(GetAndVerify(client_, *allocator_, keys[i], values[i]))
            << "After rebuild, Get/verify failed " << keys[i];
}

// ---------------------------------------------------------------------------
// 测试2(防假恢复):重建元数据须指向真实且正确的数据,不能张冠李戴
// ---------------------------------------------------------------------------
TEST_F(ClientMetadataRebuildTest, RebuiltMetadataPointsToRealData) {
    std::vector<std::pair<std::string, std::string>> kv = {
        {"distinct_A", std::string(1024, 'A')},
        {"distinct_B", std::string(2048, 'B')},
        {"distinct_C", std::string(512, 'C')},
        {"distinct_D", std::string(4096, 'D')},
    };
    std::vector<std::string> keys, values;
    for (auto& [k, v] : kv) { keys.push_back(k); values.push_back(v); }

    for (auto& [k, v] : kv) {
        auto r = PutString(client_, *allocator_, k, v);
        ASSERT_TRUE(r.has_value()) << "Put failed " << k;
    }
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, values))
        << "元数据未在窗口内全部重建";
    for (auto& [k, v] : kv)
        EXPECT_TRUE(GetAndVerify(client_, *allocator_, k, v))
            << "Rebuilt metadata for '" << k << "' points to wrong/corrupt data";
}

// ---------------------------------------------------------------------------
// 测试3(惰性删语义):被 Remove 但【空间未被复用】的 key,重建后【允许复活】,
// 且复活的数据仍正确(因为 Remove 不擦内存)。这是惰性删的预期行为,不是 bug。
// ---------------------------------------------------------------------------
// ⚠️ 语义变更说明:早期"即时删"版断言"被删 key 不复活";现改为惰性删——
//   Remove 后 client 本地表不动,已删未复用的 key 会复活,指向仍正确的旧数据。
//   本测试验证:①存活 key 正常;②被删但未复用的 key 复活了、且数据没坏(可接受)。
//   "复用后旧 key 被覆盖不复活"由测试5 验证(那才是必须保证的正确性)。
TEST_F(ClientMetadataRebuildTest, LazyDelete_RemovedButNotReused_MayRevive) {
    const int kNumKeys = 20;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("mix_key_" + std::to_string(i));
        values.push_back("mix_value_" + std::to_string(i));
        auto r = PutString(client_, *allocator_, keys.back(), values.back());
        ASSERT_TRUE(r.has_value()) << "Put failed " << keys.back();
    }
    // 删偶数下标的一半(force=true 绕 lease)。删后【不再 Put 新数据】→ 空间不被复用。
    for (int i = 0; i < kNumKeys; i += 2) {
        auto r = client_->Remove(keys[i], /*force=*/true);
        ASSERT_TRUE(r.has_value())
            << "Remove failed " << keys[i] << ": " << toString(r.error());
    }
    RestartMasterEmpty();

    // 惰性删:所有 key(含被删的)都可能重建 → 等全部。
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, keys, values))
        << "重建未完成(惰性删下被删未复用的 key 也应能复活)";

    // 断言:每个 key(不论删没删)都能 Get 到,且内容正确 —— 惰性删的预期。
    // 被删 key 复活是【可接受】的;关键是数据没坏(Remove 不擦内存)。
    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_TRUE(GetAndVerify(client_, *allocator_, keys[i], values[i]))
            << (i % 2 == 0 ? "被删未复用 key 复活后数据应正确: "
                           : "存活 key 数据应正确: ")
            << keys[i];
    }
}

// ---------------------------------------------------------------------------
// 测试4(跨 client / notify 路径,【方案核心 · 多 client】):
// clientA 不挂段、clientB 挂段 → A 的 Put 数据【必然】落到 B 段(全局池里只有 B 段)。
// 这些 key 靠 A→B 的 notify 让 B 记账;master 重启后由 B(数据物理所在者)重发重建。
// 这是最能代表"多 client 真实场景"的测试,方案的核心价值就在这里。
//
// 构造"数据必落 B 段"的可靠方法(照抄 client_integration_test.cpp 的双 client 模式:
// segment_provider_client_ 挂段、test_client_ 不挂段只 RegisterLocalMemory):
//   - clientB: MountSegment 贡献唯一可分配段。
//   - clientA: 只 RegisterLocalMemory(本地读写缓冲),【不 MountSegment】。
//   → PutStart 时全局池只有 B 段,数据【确定性】落 B,不 flaky。
//
// ⚠️ 依赖 notify 收发已实现(实现文档 2.7/2.8)。notify 未实现时:数据在 B、A 本地表
//    没有这些 key、B 也没被通知 → 重启后没人重发 → 测试红。这正是"测到了 notify"的证据。
// ⚠️ 用独立 fixture(自己起 A、B),不复用单 client 的 ClientMetadataRebuildTest。
class ClientCrossNotifyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        master_address_ = master_.master_address();

        // clientB:段 owner,挂唯一可分配段。
        auto b = Client::Create("127.0.0.1:19201", "P2PHANDSHAKE", FLAGS_protocol,
                                std::nullopt, master_address_);
        ASSERT_TRUE(b.has_value());
        clientB_ = b.value();
        segB_ = allocate_buffer_allocator_memory(kSeg);
        ASSERT_NE(segB_, nullptr);
        ASSERT_TRUE(clientB_->MountSegment(segB_, kSeg, FLAGS_protocol).has_value());

        // clientA:数据写入方,只注册本地读写缓冲,【不挂段】。
        auto a = Client::Create("127.0.0.1:19202", "P2PHANDSHAKE", FLAGS_protocol,
                                std::nullopt, master_address_);
        ASSERT_TRUE(a.has_value());
        clientA_ = a.value();
        allocA_ = std::make_unique<SimpleAllocator>(kAlloc);
        ASSERT_TRUE(clientA_->RegisterLocalMemory(allocA_->getBase(), kAlloc,
                                                  "cpu:0", false, false).has_value());
        // B 也需本地读写缓冲(它 Get 验证时用)。
        allocB_ = std::make_unique<SimpleAllocator>(kAlloc);
        ASSERT_TRUE(clientB_->RegisterLocalMemory(allocB_->getBase(), kAlloc,
                                                  "cpu:0", false, false).has_value());
    }

    void TearDown() override {
        if (clientB_ && segB_) clientB_->UnmountSegment(segB_, kSeg);
        master_.Stop();
    }

    void RestartMasterEmpty() {
        master_.Stop();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_rpc_port(master_.rpc_port())
                                      .set_http_metrics_port(
                                          master_.http_metrics_port())
                                      .build()));
    }

    static constexpr size_t kSeg = 128 * 1024 * 1024;
    static constexpr size_t kAlloc = 64 * 1024 * 1024;
    InProcMaster master_;
    std::string master_address_;
    std::shared_ptr<Client> clientA_, clientB_;
    void* segB_ = nullptr;
    std::unique_ptr<SimpleAllocator> allocA_, allocB_;
};

TEST_F(ClientCrossNotifyTest, CrossClientRebuildViaNotify) {
    const int kNumKeys = 30;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("cross_key_" + std::to_string(i));
        values.push_back("cross_val_" + std::to_string(i));
    }
    // 1. A Put(数据必落 B 段;A→B notify 让 B 记账)。
    for (int i = 0; i < kNumKeys; ++i) {
        auto r = PutString(clientA_, *allocA_, keys[i], values[i]);
        ASSERT_TRUE(r.has_value())
            << "A Put failed " << keys[i] << ": " << toString(r.error());
    }
    // (可选强断言)确认数据确实落 B 段:Query 拿副本 endpoint 与 B 比对
    // (照 client_integration_test.cpp:419-423);若 Client 暴露 GetTransportEndpoint 可解开:
    // { auto q = clientA_->Query(keys[0]); ASSERT_TRUE(q.has_value());
    //   EXPECT_EQ(q.value().replicas[0].get_memory_descriptor()
    //             .buffer_descriptor.transport_endpoint_, clientB_->GetTransportEndpoint()); }

    // 2. 基线:A 能读回(数据在 B 段,Get 经 master 查位置再 TE 读)。
    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_TRUE(GetAndVerify(clientA_, *allocA_, keys[i], values[i]))
            << "baseline A Get " << keys[i];

    // 3. master 挂 → 空重启。
    RestartMasterEmpty();

    // 4. 等重建 —— 关键:数据在 B 段,A 本地表【没有】这些 key,必须靠 B(收 notify 记了账)
    //    重发才能重建。若 notify 未生效,这里会超时红。
    ASSERT_TRUE(WaitForAllKeysRebuilt(clientA_, *allocA_, keys, values))
        << "跨 client 元数据未在窗口内重建:notify 记账 或 B 重发链路未生效";

    // 5. 【核心断言】重建后 A、B 都能读且内容正确。
    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_TRUE(GetAndVerify(clientA_, *allocA_, keys[i], values[i]))
            << "After rebuild, A Get/verify failed " << keys[i];
        EXPECT_TRUE(GetAndVerify(clientB_, *allocB_, keys[i], values[i]))
            << "After rebuild, B Get/verify failed " << keys[i];
    }
}

// ---------------------------------------------------------------------------
// 测试7(notify 可靠性兜底,§9.5.7 风险#1):notify 发送失败不能静默丢 —— 失败的
// notify 挂进 pending 队列,由后台线程重试补发,直到对端收到。若无兜底,一条丢失的
// notify 会导致 owner 漏记一份副本,master 重建时冗余静默丢失。
// ---------------------------------------------------------------------------
// 做法(确定性、可复现):A 先正常 Put 一个 key(数据落 B 段),用 Query 拿到这份指向
// B 段的【真实 Descriptor】;再用 ParkNotifyForTest 针对一个【新 key】挂起一条发往 B
// 的 notify(模拟"这条 notify 当初发失败了")。然后:
//   ① 断言 pending 桶数==1(确实挂起了);
//   ② 等后台 RebuildNotifyLoop 的 FlushPendingNotifies 补发成功 → pending 清零;
//   ③ master 重启 → 断言这个"靠补发才记上账"的新 key 也能被 B 重发重建。
// 若兜底缺失(发失败即丢),pending 永不清零、新 key 重建不出来 → 测试红。
TEST_F(ClientCrossNotifyTest, NotifyRetryBackstopRedeliversDroppedNotify) {
    // 1. A 正常 Put 一个 carrier key(落 B 段),拿它指向 B 段的真实 Descriptor。
    const std::string carrier = "carrier_key";
    const std::string carrier_val = std::string(4096, 'C');
    ASSERT_TRUE(PutString(clientA_, *allocA_, carrier, carrier_val).has_value())
        << "carrier Put failed";
    auto q = clientA_->Query(carrier);
    ASSERT_TRUE(q.has_value() && !q.value().replicas.empty())
        << "carrier Query failed";
    const Replica::Descriptor& carrier_desc = q.value().replicas.front();
    const std::string owner_ep =
        carrier_desc.get_memory_descriptor().buffer_descriptor.transport_endpoint_;

    // 2. 模拟"发往 B 的 notify 当初失败了":把一条【新 key】的 notify 挂进 pending。
    //    复用 carrier 的 Descriptor 当作该新 key 的副本位置(测试重点是补发链路,
    //    不是地址真实性;新 key 走 B 的 RecordLocalReplica → 之后能被 B 重发)。
    const std::string dropped = "dropped_notify_key";
    clientA_->ParkNotifyForTest(owner_ep, dropped, carrier_desc,
                                carrier_val.size(), ObjectDataType::UNKNOWN, "",
                                "default");

    // ① 确实挂起了。
    EXPECT_GE(clientA_->PendingNotifyBucketCountForTest(), 1u)
        << "失败的 notify 应被挂进 pending 队列(兜底缺失则不会挂起)";

    // ② 等后台线程补发成功 → pending 清零。
    bool drained = false;
    for (int i = 0; i < 40 && !drained; ++i) {
        if (clientA_->PendingNotifyBucketCountForTest() == 0) {
            drained = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    EXPECT_TRUE(drained)
        << "pending notify 未在窗口内补发清零 —— 后台重试兜底未生效";

    // 给 B 的接收线程一点时间把补发的 notify 记进本地表。
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 3. master 挂 → 空重启。
    RestartMasterEmpty();

    // ③ 断言:靠补发才记上账的 dropped key,能被 B 重发重建(master 认得它)。
    bool rebuilt = false;
    for (int i = 0; i < 40 && !rebuilt; ++i) {
        auto qq = clientA_->Query(dropped);
        if (qq.has_value() && !qq.value().replicas.empty()) {
            rebuilt = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    EXPECT_TRUE(rebuilt)
        << "补发的 notify 对应的 key 未被重建 —— 若无重试兜底,这条 notify 会丢、"
           "owner 漏记、重建时该副本静默丢失";
}

// ---------------------------------------------------------------------------
// 测试5(地址复用覆盖,惰性删的核心正确性保证):删除后空间被新 key 复用,
// 重建时被删 key 不应"复活"并指向已被新 key 占用的地址(否则静默数据损坏)。
// 对应权威文档 9.5.1。惰性删下这是【必须保证】的正确性(测试3 那种"未复用可复活"可接受,
// 但"复用后旧 key 还在"绝不可接受)。
// ---------------------------------------------------------------------------
// 原理:key_A 删除(惰性删:client 本地表【不动】)→ 其段内空间进 allocator freelist →
//      后续 Put 复用同一地址。记账时必须【按地址覆盖】——用新 key 清掉本地表里指向同一
//      (段,地址) 的 key_A 旧条目(实现文档 §2.6 RecordLocalReplica 内置 EraseByAddressLocked)。
//      本用例是【单 client 自 Put 落自己段】,走 RecordLocalReplica 的自覆盖路径(不发 notify);
//      跨 client 场景(A 写 B 段)则由 UPSERT notify 触发 B 侧同一套 RecordLocalReplica 覆盖。
//      若没做地址覆盖,重启重发会把 key_A→旧地址报上去,而该地址已装新 key 数据 → 静默损坏。
// 本测试逼迫复用并验证:①key_A 不复活(旧条目被覆盖清除);②新 key 数据完全正确。
TEST_F(ClientMetadataRebuildTest, RemovedKeySpaceReuseNoStaleMapping) {
    const std::string kA = "reuse_victim_A";
    const std::string vA = std::string(4096, 'X');   // 4KB,便于被同尺寸新 key 复用

    // 1. Put key_A 并确认可读(占用段内某地址)。
    ASSERT_TRUE(PutString(client_, *allocator_, kA, vA).has_value())
        << "Put key_A failed";
    ASSERT_TRUE(GetAndVerify(client_, *allocator_, kA, vA)) << "baseline key_A";

    // 2. force 删除 key_A(惰性删:client 本地表不动;空间归还 allocator freelist)。
    ASSERT_TRUE(client_->Remove(kA, /*force=*/true).has_value())
        << "Remove key_A failed";

    // 3. Put 一批同尺寸新 key,逼迫 allocator 复用 key_A 刚释放的地址。
    //    复用时的 UPSERT 记账应【按地址覆盖】掉 key_A 的旧本地表条目。
    const int kNumNew = 64;
    std::vector<std::string> new_keys, new_values;
    for (int i = 0; i < kNumNew; ++i) {
        new_keys.push_back("reuse_new_" + std::to_string(i));
        new_values.push_back(std::string(4096, static_cast<char>('a' + i % 26)));
        ASSERT_TRUE(
            PutString(client_, *allocator_, new_keys[i], new_values[i]).has_value())
            << "Put new key failed " << new_keys[i];
    }

    // 4. master 挂 → 空重启 → 等新 key 全部重建。
    RestartMasterEmpty();
    ASSERT_TRUE(WaitForAllKeysRebuilt(client_, *allocator_, new_keys, new_values))
        << "新 key 未在窗口内全部重建";

    // 5a. 【核心断言①】被删的 key_A 不应复活。
    {
        void* buf = allocator_->allocate(vA.size());
        std::vector<Slice> slices{Slice{buf, vA.size()}};
        auto res = client_->Get(kA, slices);
        allocator_->deallocate(buf, vA.size());
        EXPECT_FALSE(res.has_value())
            << "已删除的 key_A 复活了(删除清理逻辑漏洞)——若它还指向被新 key "
               "复用的地址,就是静默数据损坏";
    }

    // 5b. 【核心断言②】所有新 key 数据必须完全正确(没被 key_A 的陈旧映射污染)。
    for (int i = 0; i < kNumNew; ++i)
        EXPECT_TRUE(GetAndVerify(client_, *allocator_, new_keys[i], new_values[i]))
            << "新 key 数据被污染/丢失:" << new_keys[i];
}

// ---------------------------------------------------------------------------
// 测试6(多副本合并,replica_num=2):同一 key 的两份副本落在【不同段】(不同 client),
// 由各自 owner 分别重发;master 重建时必须【合并】成"该 key 有 2 份副本",而非只保留一份。
// 对应实现文档 §4(RebuildMetadata 已存在 key 走合并分支,而非 continue 跳过)。
// ---------------------------------------------------------------------------
// ⚠️ 这是"多副本冗余恢复"的唯一测试(其余测试全 replica_num=1,走不到合并分支)。
// 前提:两个 client 都 MountSegment(才有两个不同段供 replica_num=2 分散);
//       依赖 owner 记账 + 各 owner 重发 + master 合并三者都实现。
// fixture:两 client 都挂段(区别于测试4 的"A 不挂段")。
class ClientMultiReplicaTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        master_address_ = master_.master_address();
        for (int i = 0; i < 2; ++i) {
            auto c = Client::Create("127.0.0.1:1930" + std::to_string(i + 1),
                                    "P2PHANDSHAKE", FLAGS_protocol, std::nullopt,
                                    master_address_);
            ASSERT_TRUE(c.has_value());
            clients_[i] = c.value();
            seg_[i] = allocate_buffer_allocator_memory(kSeg);
            ASSERT_NE(seg_[i], nullptr);
            ASSERT_TRUE(
                clients_[i]->MountSegment(seg_[i], kSeg, FLAGS_protocol).has_value());
            alloc_[i] = std::make_unique<SimpleAllocator>(kAlloc);
            ASSERT_TRUE(clients_[i]->RegisterLocalMemory(
                alloc_[i]->getBase(), kAlloc, "cpu:0", false, false).has_value());
        }
    }
    void TearDown() override {
        for (int i = 0; i < 2; ++i)
            if (clients_[i] && seg_[i]) clients_[i]->UnmountSegment(seg_[i], kSeg);
        master_.Stop();
    }
    void RestartMasterEmpty() {
        master_.Stop();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_rpc_port(master_.rpc_port())
                                      .set_http_metrics_port(
                                          master_.http_metrics_port())
                                      .build()));
    }
    // 返回 master 记录的该 key 副本数(经 Query 拿 replicas.size())。
    int ReplicaCount(const std::string& key) {
        auto q = clients_[0]->Query(key);
        return q.has_value() ? static_cast<int>(q.value().replicas.size()) : -1;
    }
    static constexpr size_t kSeg = 128 * 1024 * 1024;
    static constexpr size_t kAlloc = 64 * 1024 * 1024;
    InProcMaster master_;
    std::string master_address_;
    std::shared_ptr<Client> clients_[2];
    void* seg_[2] = {nullptr, nullptr};
    std::unique_ptr<SimpleAllocator> alloc_[2];
};

TEST_F(ClientMultiReplicaTest, MultiReplicaMergedOnRebuild) {
    const int kNumKeys = 20;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("dual_key_" + std::to_string(i));
        values.push_back("dual_val_" + std::to_string(i));
    }
    // 1. Put replica_num=2:每个 key 两份副本,分散到两个 client 的段。
    for (int i = 0; i < kNumKeys; ++i) {
        void* buf = alloc_[0]->allocate(values[i].size());
        std::memcpy(buf, values[i].data(), values[i].size());
        std::vector<Slice> slices{Slice{buf, values[i].size()}};
        ReplicateConfig cfg; cfg.replica_num = 2;          // ★关键:2 副本
        auto r = clients_[0]->Put(keys[i], slices, cfg);
        alloc_[0]->deallocate(buf, values[i].size());
        ASSERT_TRUE(r.has_value())
            << "Put(replica_num=2) failed " << keys[i] << ": " << toString(r.error());
    }

    // 2. 基线:重启前每个 key 应有 2 份副本。
    for (int i = 0; i < kNumKeys; ++i)
        ASSERT_EQ(ReplicaCount(keys[i]), 2)
            << "baseline: key 应有 2 副本 " << keys[i];

    // 3. master 挂 → 空重启。
    RestartMasterEmpty();

    // 4. 等重建(两个 owner 各报自己那份,master 合并)。以副本数==2 为完成判据。
    bool merged = false;
    for (int attempt = 0; attempt < 40 && !merged; ++attempt) {
        merged = true;
        for (int i = 0; i < kNumKeys; ++i)
            if (ReplicaCount(keys[i]) != 2) { merged = false; break; }
        if (!merged) std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // 5. 【核心断言】重建后每个 key 恢复成 2 份副本(合并成功,冗余未丢)。
    //    若 master 用 continue 跳过(旧代码),这里会是 1 → 测试红。
    for (int i = 0; i < kNumKeys; ++i)
        EXPECT_EQ(ReplicaCount(keys[i]), 2)
            << "重建后 key 副本数应为 2(多副本合并):" << keys[i]
            << " —— 若为 1 说明 master 未合并、丢了第二份副本(冗余丢失)";

    // 6. 数据仍可读且正确。
    for (int i = 0; i < kNumKeys; ++i) {
        void* buf = alloc_[0]->allocate(values[i].size());
        std::vector<Slice> slices{Slice{buf, values[i].size()}};
        auto res = clients_[0]->Get(keys[i], slices);
        bool ok = res.has_value() &&
                  std::memcmp(slices[0].ptr, values[i].data(), values[i].size()) == 0;
        alloc_[0]->deallocate(buf, values[i].size());
        EXPECT_TRUE(ok) << "重建后数据应正确 " << keys[i];
    }
}

}  // namespace testing
}  // namespace mooncake
