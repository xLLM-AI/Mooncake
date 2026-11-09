#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

#include "client_service.h"
#include "client_test_peer.h"
#include "config.h"
#include "transfer_engine.h"

namespace mooncake {

namespace {
class ScopedInitEnv {
   public:
    explicit ScopedInitEnv(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) previous_ = value;
        unsetenv(name);
    }
    ~ScopedInitEnv() {
        if (previous_) {
            setenv(name_.c_str(), previous_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }
    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> previous_;
};

class ClientTransportInitTest : public ::testing::Test {
   protected:
    ScopedInitEnv auto_disc{"MC_MS_AUTO_DISC"};
    ScopedInitEnv filters{"MC_MS_FILTERS"};
    ScopedInitEnv json{"MC_CUSTOM_TOPO_JSON"};
    ScopedInitEnv force_tcp{"MC_FORCE_TCP"};
    ScopedInitEnv force_shm{"MC_FORCE_SHM"};
    ScopedInitEnv tent{"MC_USE_TENT"};
    ScopedInitEnv te_filters{"MC_TE_FILTERS"};
    std::string path;

    void SetUp() override {
        char file[] = "/tmp/mooncake-store-topology-XXXXXX";
        int fd = mkstemp(file);
        ASSERT_GE(fd, 0);
        close(fd);
        path = file;
        std::ofstream out(path);
        // These fictional HCAs cannot come from system discovery. The two
        // priority tiers also detect accidentally rebuilding the JSON matrix.
        out << R"({"cpu:0":[["store_test_hca_a"],["store_test_hca_b"]]})";
        out.close();
        json.Set(path.c_str());
    }
    void TearDown() override { unlink(path.c_str()); }
};

TEST_F(ClientTransportInitTest, AutoDiscoveryIgnoresStaleExplicitDevices) {
    auto_disc.Set("1");
    filters.Set("store_test_hca_b");
    auto client = ClientTransportInitTestPeer::Make();
    ClientTransportInitTestPeer::Init(*client, "rdma", "stale_device");
    auto topology =
        ClientTransportInitTestPeer::Engine(*client).getLocalTopology();
    EXPECT_EQ(topology->getHcaList(),
              std::vector<std::string>{"store_test_hca_b"});
}

TEST_F(ClientTransportInitTest, AutoDiscoveryRetainsCustomTopologyDevices) {
    auto_disc.Set("1");
    auto client = ClientTransportInitTestPeer::Make();
    ClientTransportInitTestPeer::Init(*client, "rdma", std::nullopt);
    auto topology =
        ClientTransportInitTestPeer::Engine(*client).getLocalTopology();
    // RDMA legitimately disables these fictional devices during installation,
    // removing matrix entries but retaining the discovered device slots.
    // Matrix fidelity before installation is covered in TransportTest.
    EXPECT_EQ(
        topology->getHcaList(),
        (std::vector<std::string>{"store_test_hca_a", "store_test_hca_b"}));
}

TEST_F(ClientTransportInitTest, DisabledDiscoveryRejectsMissingOrEmptyDevices) {
    auto_disc.Set("0");
    for (const auto& devices :
         {std::optional<std::string>{}, std::optional<std::string>{""}}) {
        auto client = ClientTransportInitTestPeer::Make();
        EXPECT_EQ(ClientTransportInitTestPeer::Init(*client, "rdma", devices),
                  ErrorCode::INVALID_PARAMS);
        EXPECT_TRUE(ClientTransportInitTestPeer::Engine(*client)
                        .getLocalTopology()
                        ->getHcaList()
                        .empty());
        EXPECT_TRUE(ClientTransportInitTestPeer::Engine(*client)
                        .autoInstallTransportEnabled());
    }
}

TEST_F(ClientTransportInitTest,
       DisabledDiscoveryWhitelistCannotReplaceDevices) {
    auto_disc.Set("0");
    filters.Set("store_test_hca_b");
    for (const auto& devices :
         {std::optional<std::string>{}, std::optional<std::string>{""}}) {
        auto client = ClientTransportInitTestPeer::Make();
        EXPECT_EQ(ClientTransportInitTestPeer::Init(*client, "rdma", devices),
                  ErrorCode::INVALID_PARAMS);
        // The whitelist cannot turn manual mode into unfiltered discovery.
        EXPECT_TRUE(ClientTransportInitTestPeer::Engine(*client)
                        .getLocalTopology()
                        ->getHcaList()
                        .empty());
    }
}

TEST_F(ClientTransportInitTest,
       DefaultDiscoveryDistinguishesAbsentAndEmptyDevices) {
    {
        auto client = ClientTransportInitTestPeer::Make();
        EXPECT_NE(
            ClientTransportInitTestPeer::Init(*client, "rdma", std::nullopt),
            ErrorCode::INVALID_PARAMS);
        EXPECT_EQ(
            ClientTransportInitTestPeer::Engine(*client)
                .getLocalTopology()
                ->getHcaList(),
            (std::vector<std::string>{"store_test_hca_a", "store_test_hca_b"}));
    }
    {
        auto client = ClientTransportInitTestPeer::Make();
        EXPECT_EQ(ClientTransportInitTestPeer::Init(*client, "rdma", ""),
                  ErrorCode::INVALID_PARAMS);
        EXPECT_TRUE(ClientTransportInitTestPeer::Engine(*client)
                        .getLocalTopology()
                        ->getHcaList()
                        .empty());
    }
}

TEST_F(ClientTransportInitTest, ManualSelectionDoesNotLoadCustomJson) {
    auto_disc.Set("0");
    auto client = ClientTransportInitTestPeer::Make();
    EXPECT_NE(
        ClientTransportInitTestPeer::Init(*client, "rdma", "stale_device"),
        ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(ClientTransportInitTestPeer::Engine(*client)
                    .getLocalTopology()
                    ->getHcaList()
                    .empty());
}

TEST_F(ClientTransportInitTest,
       UnmatchedJsonWhitelistFailsWithoutSystemFallback) {
    auto_disc.Set("1");
    filters.Set("not_in_json");
    auto client = ClientTransportInitTestPeer::Make();
    EXPECT_EQ(
        ClientTransportInitTestPeer::Init(*client, "rdma", "stale_device"),
        ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(ClientTransportInitTestPeer::Engine(*client)
                    .getLocalTopology()
                    ->getHcaList()
                    .empty());
}

TEST_F(ClientTransportInitTest, ForceTcpBypassesHardwareAndDeviceValidation) {
    force_tcp.Set("1");
    auto_disc.Set("0");
    for (const char* protocol : {"rdma", "ascend"}) {
        auto client = ClientTransportInitTestPeer::Make();
        ASSERT_EQ(
            ClientTransportInitTestPeer::Init(*client, protocol, std::nullopt),
            ErrorCode::OK);
        auto& engine = ClientTransportInitTestPeer::Engine(*client);
        EXPECT_NE(engine.getTransport("tcp"), nullptr);
        EXPECT_EQ(engine.getTransport(protocol), nullptr);
        EXPECT_EQ(engine.getTransport("shm"), nullptr);
        EXPECT_TRUE(engine.autoInstallTransportEnabled());
    }
}

TEST_F(ClientTransportInitTest, ForceShmPreservesSingleOrCompanionMode) {
    force_shm.Set("1");
    auto_disc.Set("0");
    auto client = ClientTransportInitTestPeer::Make();
    ASSERT_EQ(ClientTransportInitTestPeer::Init(*client, "tcp", std::nullopt),
              ErrorCode::OK);
    auto& engine = ClientTransportInitTestPeer::Engine(*client);
    EXPECT_NE(engine.getTransport("shm"), nullptr);
#ifdef ENABLE_MULTI_PROTOCOL
    EXPECT_NE(engine.getTransport("tcp"), nullptr);
#else
    EXPECT_EQ(engine.getTransport("tcp"), nullptr);
#endif
}

TEST_F(ClientTransportInitTest, TcpForceHasPriorityOverShmForce) {
    force_tcp.Set("1");
    force_shm.Set("1");
    auto client = ClientTransportInitTestPeer::Make();
    ASSERT_EQ(ClientTransportInitTestPeer::Init(*client, "rdma", std::nullopt),
              ErrorCode::OK);
    auto& engine = ClientTransportInitTestPeer::Engine(*client);
    EXPECT_NE(engine.getTransport("tcp"), nullptr);
    EXPECT_EQ(engine.getTransport("rdma"), nullptr);
    EXPECT_EQ(engine.getTransport("shm"), nullptr);
}
}  // namespace
}  // namespace mooncake
