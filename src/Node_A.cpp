#include "telemetry_pipeline.pb.h"
#include "telemetry_pipeline.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/alarm.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <random>

class Client_NodeA final {
public:
    explicit Client_NodeA(std::shared_ptr<grpc::Channel> channel) : channel_(channel),
                                                                stub_(telemetry::AccelService::NewStub(channel_)) {}

    ~Client_NodeA() {
        std::unique_lock<std::mutex> u_lock(mtx_);
        cv.wait(u_lock, [&] { return count_of_running_rpcs_ == 0; });
    }

    void StreamAccelData() {
        class Chatter final : public grpc::ClientBidiReactor<telemetry::AccelPacket, telemetry::AccelModule> {
            public:
                Chatter(Client_NodeA* client) : client_(client) {
                    // std::cout << "gaz" << std::endl;
                    client_->stub_->async()->StreamAccelData(&context_, this);
                    AddHold();
                    NextWrite();
                    StartRead(&received_);
                    StartCall();
                }

                ~Chatter() = default;

                void OnWriteDone(bool ok) override {
                    std::cout << "done" << std::endl;
                    if(ok) {
                        alarm_.Set(std::chrono::system_clock::now() + std::chrono::seconds(1), [this](bool ) { NextWrite(); });
                    } else {    

                    }
                }

                void OnReadDone(bool ok) override {
                    std::cout << "AccelModule: time - " << received_.time() << " , " << received_.module() << " \n";
                    StartRead(&received_);
                }

                void OnDone(const grpc::Status& s) override {
                    // std::unique_lock<std::mutex> l(client_->mtx_);
                    // if(client_->count_of_running_rpcs_ == 0) {
                    //     client_->cv.notify_one();
                    // }
                    --client_->count_of_running_rpcs_; // можно просто оповестить всех 
                    client_->cv.notify_one();
                    delete this;
                }

                private:
                void NextWrite() {
                    auto now = std::chrono::system_clock::now();
                    to_deliver_.set_time(std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
                    to_deliver_.set_x(rand() % 1000);
                    to_deliver_.set_y(rand() % 1000);
                    to_deliver_.set_z(rand() % 1000);
                    StartWrite(&to_deliver_);
                }

            private:
                Client_NodeA* client_ = nullptr;
                grpc::ClientContext context_;

                grpc::Alarm alarm_;

                telemetry::AccelPacket to_deliver_;
                telemetry::AccelModule received_;
        };
        ++count_of_running_rpcs_;
        Chatter* chat = new Chatter(this);
    }

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<telemetry::AccelService::Stub> stub_;

    std::atomic<int> count_of_running_rpcs_{0};
    std::mutex mtx_;
    std::condition_variable cv;
};

int main() {
    Client_NodeA client(grpc::CreateChannel("localhost:57780", grpc::InsecureChannelCredentials()));
    client.StreamAccelData();
}