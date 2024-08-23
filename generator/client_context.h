#pragma once

#include <clickhouse/client.h>

namespace chfuzz {

class ClientContext {
public:
	clickhouse::Client client;

	ClientContext(const clickhouse::ClientOptions opt) : client(opt) {}
};

}
