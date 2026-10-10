cmake_minimum_required(VERSION 3.16)
project(Telemetry_Pipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 1. Находим Protobuf и gRPC
find_package(Protobuf REQUIRED CONFIG)
find_package(gRPC REQUIRED CONFIG)

# 2. Настраиваем разделение папок (Используем ОТНОСИТЕЛЬНЫЕ пути для protoc)
set(PROTO_DIR "proto")      # Просто имя папки, без CMAKE_CURRENT_SOURCE_DIR
set(GEN_DIR "proto_src")    # Просто имя папки

# Физически создаем папку для исходного кода, если её нет
file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${GEN_DIR}")

# Полный путь к файлу нужен только для зависимостей CMake
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_DIR}/telemetry_pipeline.proto")

# 3. Список генерируемых файлов (здесь оставляем полные пути, чтобы CMake их нашел)
set(GENERATED_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/${GEN_DIR}/telemetry_pipeline.pb.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/${GEN_DIR}/telemetry_pipeline.pb.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/${GEN_DIR}/telemetry_pipeline.grpc.pb.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/${GEN_DIR}/telemetry_pipeline.grpc.pb.h"
)

set_source_files_properties(${GENERATED_SOURCES} PROPERTIES GENERATED TRUE)

# 4. Команда автоматической генерации
# Запуск происходит из корневой директории проекта (WORKING_DIRECTORY)
add_custom_command(
    OUTPUT ${GENERATED_SOURCES}
    COMMAND protobuf::protoc
            --proto_path=${PROTO_DIR}
            --cpp_out=${GEN_DIR}
            --grpc_out=${GEN_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            ${PROTO_DIR}/telemetry_pipeline.proto
    DEPENDS "${PROTO_FILE}"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" # Важно: запускаем строго из корня!
    COMMENT "CMake: Автоматическая генерация C++ и gRPC кода для ${PROTO_DIR}/telemetry_pipeline.proto"
    VERBATIM
)

# 5. Создаем С++ библиотеку
add_library(telemetry_proto_lib STATIC ${GENERATED_SOURCES})

# 6. Привязываем зависимости
target_link_libraries(telemetry_proto_lib PUBLIC gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(telemetry_proto_lib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/${GEN_DIR}")


add_executable(NodeA src/Node_A.cpp)
add_executable(NodeB src/Node_B.cpp)
add_executable(server src/server.cpp)

add_dependencies(NodeA telemetry_proto_lib)
add_dependencies(NodeB telemetry_proto_lib)
add_dependencies(server telemetry_proto_lib)

# Привязываем библиотеки (здесь лучше использовать PRIVATE для исполняемых файлов)
target_link_libraries(NodeA PRIVATE telemetry_proto_lib)
target_link_libraries(NodeB PRIVATE telemetry_proto_lib)
target_link_libraries(server PRIVATE telemetry_proto_lib)