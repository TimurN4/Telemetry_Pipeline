#include "telemetry_pipeline.pb.h"
#include "telemetry_pipeline.grpc.pb.h"

#include <grpcpp/server_builder.h>
#include <google/protobuf/util/message_differencer.h> 

#include <iostream>
#include <string>
#include <chrono>
#include <cmath>


class AccelCalcServiceImpl final : public telemetry::AccelCalcService::CallbackService {
public:
    explicit AccelCalcServiceImpl() = default;

    ~AccelCalcServiceImpl() = default;

    grpc::ServerBidiReactor<telemetry::AccelPacket, telemetry::AccelModule>* StreamAccelData(
    grpc::CallbackServerContext* context) {
    class Chatter : public grpc::ServerBidiReactor<telemetry::AccelPacket, telemetry::AccelModule> {
    public:
        Chatter() {
            StartRead(&last_received_);
        }

        void OnReadDone(bool ok) override {
            if (ok) {
                NextWrite();
            } else {
                Finish(grpc::Status::OK);
            }
        }

        void OnWriteDone(bool ok) override { 
            if(ok) {
                StartRead(&last_received_);
            } else {
                Finish(grpc::Status::OK);
            }
        }

        void OnDone() override {
            std::cout << "RPC Completed";
            delete this;
        }

        void OnCancel() override { 
            std::cout << "RPC Cancelled"; 
        }

    private:
        void NextWrite() {
            to_deliver_.set_time(last_received_.time());
            auto module = std::sqrt(last_received_.x() * last_received_.x() + 
                                    last_received_.y() * last_received_.y() + 
                                    last_received_.z() * last_received_.z());
            to_deliver_.set_module(module);
            StartWrite(&to_deliver_);
        }

    private:
        telemetry::AccelPacket last_received_;
        telemetry::AccelModule to_deliver_;
    };
    return new Chatter();
}
private:

};

void RunServer() {
    std::string server_adress = "127.0.0.1:57781";
    AccelCalcServiceImpl service;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_adress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "server started\n";
    server->Wait();
}

int main() {
    RunServer();
    return 0;
}