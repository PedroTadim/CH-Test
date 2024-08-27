cmake_minimum_required(VERSION 3.12)

project(CH-Test)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(contribs/clickhouse-cpp)

find_package(Protobuf REQUIRED)

PROTOBUF_GENERATE_CPP(FUNCS_SRC FUNCS_HDR ast/sql_funcs.proto)
PROTOBUF_GENERATE_CPP(GRAMMAR_SRC GRAMMAR_HDR ast/sql_grammar.proto)

add_executable(ast_fuzz
fuzz.cpp
ast/sql_query_proto_to_string.cpp
generator/sql_query.cpp
generator/sql_expression.cpp
generator/sql_types.cpp
generator/statement_generator.cpp
${FUNCS_SRC} ${FUNCS_HDR} ${GRAMMAR_SRC} ${GRAMMAR_HDR})

target_include_directories(ast_fuzz PRIVATE
contribs/clickhouse-cpp
contribs/clickhouse-cpp/contrib/absl
${Protobuf_INCLUDE_DIRS}
${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(ast_fuzz PRIVATE
clickhouse-cpp-lib
${Protobuf_LIBRARIES})
