#include <gtest/gtest.h>
#include <grpc++/grpc++.h>
#include <thread>
#include <chrono>

#include "cache_service.grpc.pb.h"
#include "distcache/storage_engine.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using distcache::v1::CacheService;
using distcache::v1::GetRequest;
using distcache::v1::GetResponse;
using distcache::v1::SetRequest;
using distcache::v1::SetResponse;
using distcache::v1::DeleteRequest;
using distcache::v1::DeleteResponse;
using distcache::v1::HealthCheckRequest;
using distcache::v1::HealthCheckResponse;

namespace distcache {

// Test implementation of CacheService
class CacheServiceImpl final : public CacheService::Service {
public:
    CacheServiceImpl() : storage_(256, 1024 * 1024 * 1024) {}

    Status Get(ServerContext* /* context */, const GetRequest* request,
               GetResponse* response) override {
        auto entry = storage_.get(request->key());

        if (entry.has_value()) {
            response->set_found(true);
            response->set_value(entry->value.data(), entry->value.size());
            response->set_version(entry->version);
        } else {
            response->set_found(false);
        }

        return Status::OK;
    }

    Status Set(ServerContext* /* context */, const SetRequest* request,
               SetResponse* response) override {
        std::vector<uint8_t> value(request->value().begin(), request->value().end());

        std::optional<int32_t> ttl;
        if (request->has_ttl_seconds()) {
            ttl = request->ttl_seconds();
        }

        CacheEntry entry(request->key(), std::move(value), ttl);

        bool success = storage_.set(request->key(), std::move(entry));
        response->set_success(success);
        response->set_version(1);

        return Status::OK;
    }

    Status Delete(ServerContext* /* context */, const DeleteRequest* request,
                  DeleteResponse* response) override {
        bool success = storage_.del(request->key());
        response->set_success(success);

        return Status::OK;
    }

    Status HealthCheck(ServerContext* /* context */, const HealthCheckRequest* /* request */,
                       HealthCheckResponse* response) override {
        response->set_status(HealthCheckResponse::SERVING);
        response->set_message("Cache server is healthy");
        return Status::OK;
    }

private:
    ShardedHashTable storage_;
};

// Test fixture for gRPC integration tests
class GrpcServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Start server on a random available port
        std::string server_address = "localhost:0";

        ServerBuilder builder;
        int selected_port;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &selected_port);
        builder.RegisterService(&service_);

        server_ = builder.BuildAndStart();
        ASSERT_NE(server_, nullptr) << "Failed to start server";

        // Store the actual port that was selected
        server_port_ = selected_port;

        // Create channel to server
        std::string target = "localhost:" + std::to_string(server_port_);
        channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());

        // Create stub
        stub_ = CacheService::NewStub(channel_);

        // Wait for channel to be ready
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        ASSERT_TRUE(channel_->WaitForConnected(deadline)) << "Failed to connect to server";
    }

    void TearDown() override {
        server_->Shutdown();
        server_->Wait();
    }

    CacheServiceImpl service_;
    std::unique_ptr<Server> server_;
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<CacheService::Stub> stub_;
    int server_port_;
};

// ====================
// Basic Operations Tests
// ====================

TEST_F(GrpcServerTest, BasicSetAndGet) {
    // Set a value
    SetRequest set_req;
    set_req.set_key("test_key");
    set_req.set_value("hello world");

    SetResponse set_resp;
    ClientContext set_ctx;

    Status status = stub_->Set(&set_ctx, set_req, &set_resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(set_resp.success());

    // Get the value back
    GetRequest get_req;
    get_req.set_key("test_key");

    GetResponse get_resp;
    ClientContext get_ctx;

    status = stub_->Get(&get_ctx, get_req, &get_resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "hello world");
}

TEST_F(GrpcServerTest, GetNonExistentKey) {
    GetRequest req;
    req.set_key("does_not_exist");

    GetResponse resp;
    ClientContext ctx;

    Status status = stub_->Get(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_FALSE(resp.found());
}

TEST_F(GrpcServerTest, DeleteKey) {
    // First, set a key
    SetRequest set_req;
    set_req.set_key("delete_me");
    set_req.set_value("temporary");

    SetResponse set_resp;
    ClientContext set_ctx;
    stub_->Set(&set_ctx, set_req, &set_resp);

    // Delete it
    DeleteRequest del_req;
    del_req.set_key("delete_me");

    DeleteResponse del_resp;
    ClientContext del_ctx;

    Status status = stub_->Delete(&del_ctx, del_req, &del_resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(del_resp.success());

    // Verify it's gone
    GetRequest get_req;
    get_req.set_key("delete_me");

    GetResponse get_resp;
    ClientContext get_ctx;

    stub_->Get(&get_ctx, get_req, &get_resp);
    EXPECT_FALSE(get_resp.found());
}

TEST_F(GrpcServerTest, UpdateExistingKey) {
    // Set initial value
    SetRequest req1;
    req1.set_key("update_key");
    req1.set_value("original");

    SetResponse resp1;
    ClientContext ctx1;
    stub_->Set(&ctx1, req1, &resp1);

    // Update the value
    SetRequest req2;
    req2.set_key("update_key");
    req2.set_value("updated");

    SetResponse resp2;
    ClientContext ctx2;
    stub_->Set(&ctx2, req2, &resp2);

    // Get and verify
    GetRequest get_req;
    get_req.set_key("update_key");

    GetResponse get_resp;
    ClientContext get_ctx;

    stub_->Get(&get_ctx, get_req, &get_resp);
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "updated");
}

// ====================
// TTL Tests
// ====================

TEST_F(GrpcServerTest, SetWithTTL) {
    SetRequest req;
    req.set_key("ttl_key");
    req.set_value("expires soon");
    req.set_ttl_seconds(0); // Expires immediately

    SetResponse resp;
    ClientContext ctx;

    Status status = stub_->Set(&ctx, req, &resp);
    ASSERT_TRUE(status.ok());

    // Wait a bit for expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Try to get - should not be found
    GetRequest get_req;
    get_req.set_key("ttl_key");

    GetResponse get_resp;
    ClientContext get_ctx;

    stub_->Get(&get_ctx, get_req, &get_resp);
    EXPECT_FALSE(get_resp.found());
}

TEST_F(GrpcServerTest, LongTTLDoesNotExpire) {
    SetRequest req;
    req.set_key("long_ttl_key");
    req.set_value("stays for a while");
    req.set_ttl_seconds(3600); // 1 hour

    SetResponse resp;
    ClientContext ctx;

    stub_->Set(&ctx, req, &resp);

    // Get immediately - should be found
    GetRequest get_req;
    get_req.set_key("long_ttl_key");

    GetResponse get_resp;
    ClientContext get_ctx;

    stub_->Get(&get_ctx, get_req, &get_resp);
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "stays for a while");
}

// ====================
// Concurrent Client Tests
// ====================

TEST_F(GrpcServerTest, ConcurrentWrites) {
    const int num_threads = 10;
    const int writes_per_thread = 50;

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, writes_per_thread]() {
            auto local_stub = CacheService::NewStub(channel_);

            for (int i = 0; i < writes_per_thread; ++i) {
                SetRequest req;
                req.set_key("thread_" + std::to_string(t) + "_key_" + std::to_string(i));
                req.set_value("value_" + std::to_string(i));

                SetResponse resp;
                ClientContext ctx;

                Status status = local_stub->Set(&ctx, req, &resp);
                EXPECT_TRUE(status.ok());
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all keys are present
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < writes_per_thread; ++i) {
            GetRequest req;
            req.set_key("thread_" + std::to_string(t) + "_key_" + std::to_string(i));

            GetResponse resp;
            ClientContext ctx;

            stub_->Get(&ctx, req, &resp);
            EXPECT_TRUE(resp.found());
        }
    }
}

TEST_F(GrpcServerTest, ConcurrentReads) {
    // Pre-populate data
    for (int i = 0; i < 100; ++i) {
        SetRequest req;
        req.set_key("read_key_" + std::to_string(i));
        req.set_value("read_value_" + std::to_string(i));

        SetResponse resp;
        ClientContext ctx;
        stub_->Set(&ctx, req, &resp);
    }

    const int num_threads = 10;
    std::atomic<int> successful_reads{0};

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, &successful_reads]() {
            auto local_stub = CacheService::NewStub(channel_);

            for (int i = 0; i < 100; ++i) {
                GetRequest req;
                req.set_key("read_key_" + std::to_string(i));

                GetResponse resp;
                ClientContext ctx;

                Status status = local_stub->Get(&ctx, req, &resp);
                if (status.ok() && resp.found()) {
                    successful_reads++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(successful_reads.load(), num_threads * 100);
}

// ====================
// Health Check Tests
// ====================

TEST_F(GrpcServerTest, HealthCheck) {
    HealthCheckRequest req;
    HealthCheckResponse resp;
    ClientContext ctx;

    Status status = stub_->HealthCheck(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(resp.status(), HealthCheckResponse::SERVING);
    EXPECT_FALSE(resp.message().empty());
}

// ====================
// Edge Cases
// ====================

TEST_F(GrpcServerTest, EmptyKey) {
    SetRequest req;
    req.set_key("");
    req.set_value("value");

    SetResponse resp;
    ClientContext ctx;

    Status status = stub_->Set(&ctx, req, &resp);
    EXPECT_TRUE(status.ok());
}

TEST_F(GrpcServerTest, EmptyValue) {
    SetRequest req;
    req.set_key("empty_val_key");
    req.set_value("");

    SetResponse resp;
    ClientContext ctx;

    Status status = stub_->Set(&ctx, req, &resp);
    EXPECT_TRUE(status.ok());

    GetRequest get_req;
    get_req.set_key("empty_val_key");

    GetResponse get_resp;
    ClientContext get_ctx;

    stub_->Get(&get_ctx, get_req, &get_resp);
    EXPECT_TRUE(get_resp.found());
    EXPECT_TRUE(get_resp.value().empty());
}

TEST_F(GrpcServerTest, LargeValue) {
    std::string large_value(100000, 'x'); // 100KB

    SetRequest req;
    req.set_key("large_key");
    req.set_value(large_value);

    SetResponse resp;
    ClientContext ctx;

    Status status = stub_->Set(&ctx, req, &resp);
    ASSERT_TRUE(status.ok());

    GetRequest get_req;
    get_req.set_key("large_key");

    GetResponse get_resp;
    ClientContext get_ctx;

    status = stub_->Get(&get_ctx, get_req, &get_resp);
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value().size(), 100000);
}

TEST_F(GrpcServerTest, SpecialCharactersInKey) {
    std::string special_key = "key:with:special/chars?query=value&param=data";

    SetRequest req;
    req.set_key(special_key);
    req.set_value("special value");

    SetResponse resp;
    ClientContext ctx;

    stub_->Set(&ctx, req, &resp);

    GetRequest get_req;
    get_req.set_key(special_key);

    GetResponse get_resp;
    ClientContext get_ctx;

    stub_->Get(&get_ctx, get_req, &get_resp);
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "special value");
}

TEST_F(GrpcServerTest, ManySequentialOperations) {
    // Simulate real-world usage pattern
    for (int i = 0; i < 1000; ++i) {
        std::string key = "seq_key_" + std::to_string(i);
        std::string value = "seq_value_" + std::to_string(i);

        // Set
        SetRequest set_req;
        set_req.set_key(key);
        set_req.set_value(value);

        SetResponse set_resp;
        ClientContext set_ctx;
        EXPECT_TRUE(stub_->Set(&set_ctx, set_req, &set_resp).ok());

        // Get
        GetRequest get_req;
        get_req.set_key(key);

        GetResponse get_resp;
        ClientContext get_ctx;
        EXPECT_TRUE(stub_->Get(&get_ctx, get_req, &get_resp).ok());
        EXPECT_TRUE(get_resp.found());

        // Delete every 10th key
        if (i % 10 == 0) {
            DeleteRequest del_req;
            del_req.set_key(key);

            DeleteResponse del_resp;
            ClientContext del_ctx;
            EXPECT_TRUE(stub_->Delete(&del_ctx, del_req, &del_resp).ok());
        }
    }
}

} // namespace distcache
