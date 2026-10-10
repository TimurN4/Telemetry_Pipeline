#include "telemetry_pipeline.pb.h"
#include "telemetry_pipeline.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <memory>
#include <queue>
#include <iostream>

class Session_NodeA;

// =====================================================================
// ПРАВАЯ РУКА: Реактор для работы с Узлом Б (Клиентская часть)
// =====================================================================
class Session_NodeB : public grpc::ClientBidiReactor<telemetry::AccelPacket, telemetry::AccelModule>,
                      public std::enable_shared_from_this<Session_NodeB> {
public:
    Session_NodeB(telemetry::AccelCalcService::Stub* stub, std::shared_ptr<Session_NodeA> node_a) 
        : stub_(stub), node_a_weak_(node_a) {}

    void Start() {
        // Удерживаем сами себя в памяти, пока gRPC не вызовет OnDone
        self_ref_ = shared_from_this(); 
        
        stub_->async()->StreamAccelData(&context_, this);
        StartRead(&incoming_from_B_);
        StartCall();
    }

    void SendToNodeB(const telemetry::AccelPacket& packet) {
        queue_to_B_.push(packet);
        if(!write_in_flight_to_B_) {
            write_in_flight_to_B_ = true;
            StartWrite(&queue_to_B_.front());
        }
    }

    void OnReadDone(bool ok) override;

    void OnWriteDone(bool ok) override {
        if(!ok) return; 
        queue_to_B_.pop();
        if(queue_to_B_.empty()) {
            write_in_flight_to_B_ = false;
        } else {
            StartWrite(&queue_to_B_.front());
        }
    }

    void CancelRPC() {
        context_.TryCancel();
    }

    void OnDone(const grpc::Status& status) override {
        // gRPC закончил работу с этим реактором. Отпускаем ссылку на себя.
        // Если Узел А тоже нас отпустил, объект безопасно уничтожится.
        self_ref_.reset(); 
    }

private:
    telemetry::AccelCalcService::Stub* stub_;
    grpc::ClientContext context_;
    
    std::shared_ptr<Session_NodeB> self_ref_; // Самоудержание
    std::weak_ptr<Session_NodeA> node_a_weak_; // Слабая ссылка на левую руку
    
    telemetry::AccelModule incoming_from_B_; 
    bool write_in_flight_to_B_ = false;
    std::queue<telemetry::AccelPacket> queue_to_B_; 
};

// =====================================================================
// ЛЕВАЯ РУКА: Реактор для работы с Узлом А (Серверная часть)
// =====================================================================
class Session_NodeA : public grpc::ServerBidiReactor<telemetry::AccelPacket, telemetry::AccelModule>,
                      public std::enable_shared_from_this<Session_NodeA> {
public:
    Session_NodeA() {}

    void Start(telemetry::AccelCalcService::Stub* stub_for_B) {
        self_ref_ = shared_from_this(); // Удерживаем себя для gRPC

        // Создаем правую руку и передаем ей shared_ptr на нас
        auto node_b = std::make_shared<Session_NodeB>(stub_for_B, shared_from_this());
        node_b_weak_ = node_b; // Запоминаем слабую ссылку, чтобы не было утечек
        
        node_b->Start();
        StartRead(&incoming_from_A_);
    }

    void SendToNodeA(const telemetry::AccelModule& module) {
        queue_to_A_.push(module);
        if(!write_in_flight_to_A_) {
            write_in_flight_to_A_ = true;
            StartWrite(&queue_to_A_.front());
        }
    }

    void OnReadDone(bool ok) override {
        if(!ok) return; 
        
        if (auto node_b = node_b_weak_.lock()) {
            node_b->SendToNodeB(incoming_from_A_);
        }
        StartRead(&incoming_from_A_);
    }

    void OnWriteDone(bool ok) override {
        if(!ok) return; 
        queue_to_A_.pop();
        if(queue_to_A_.empty()) {
            write_in_flight_to_A_ = false;
        } else {
            StartWrite(&queue_to_A_.front());
        }
    }

    void OnCancel() override {
        // Узел А отвалился. Гасим Узел Б.
        if (auto node_b = node_b_weak_.lock()) {
            node_b->CancelRPC(); 
        }
    }

    void OnDone() override {
        // На случай, если OnCancel не вызвался, дублируем отмену
        if (auto node_b = node_b_weak_.lock()) {
            node_b->CancelRPC(); 
        }
        // Отпускаем ссылку. Объект удалится автоматически.
        self_ref_.reset(); 
    }

private:
    std::shared_ptr<Session_NodeA> self_ref_;
    std::weak_ptr<Session_NodeB> node_b_weak_; 
    
    telemetry::AccelPacket incoming_from_A_; 
    bool write_in_flight_to_A_ = false;
    std::queue<telemetry::AccelModule> queue_to_A_; 
};

void Session_NodeB::OnReadDone(bool ok) {
    if(!ok) return; 
    
    // Проверяем, жив ли еще Узел А, прежде чем отправлять ему данные
    if (auto node_a = node_a_weak_.lock()) {
        node_a->SendToNodeA(incoming_from_B_);
    }
    StartRead(&incoming_from_B_);
}

// =====================================================================
// САМ СЕРВИС
// =====================================================================
class AccelServiceImpl final : public telemetry::AccelService::CallbackService {
public:
    AccelServiceImpl() 
        // ВНИМАНИЕ: Порт клиента изменен на 57782, чтобы прокси не стучался сам в себя!
        : channel_(grpc::CreateChannel("localhost:57781", grpc::InsecureChannelCredentials())),
          stub_for_B_(telemetry::AccelCalcService::NewStub(channel_)) {}

    grpc::ServerBidiReactor<telemetry::AccelPacket, telemetry::AccelModule>* StreamAccelData(
        grpc::CallbackServerContext* context) override {
        
        // Создаем через make_shared, чтобы работал shared_from_this()
        auto session = std::make_shared<Session_NodeA>();
        session->Start(stub_for_B_.get());
        
        // Возвращаем сырой указатель (gRPC это требует), 
        // но память защищена внутренним self_ref_
        return session.get(); 
    }

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<telemetry::AccelCalcService::Stub> stub_for_B_;
};

void RunServer() {
    std::string server_adress = "0.0.0.0:57780"; // Слушаем все интерфейсы
    AccelServiceImpl service;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_adress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Proxy server started on " << server_adress << "\n";
    server->Wait();
}

int main() {
    RunServer();
    return 0;
}