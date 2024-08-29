#pragma once

#include <string>
#include <fstream>

#include <clickhouse/client.h>

namespace chfuzz {

class ClientContext {
private:
	std::ofstream outf;

public:
	clickhouse::Client client;

	ClientContext(const clickhouse::ClientOptions opt) : client(opt),
														 outf("/tmp/out.sql", std::ios::out | std::ios::trunc) {}

	void LogQuery(const std::string &query) {
		outf << query << std::endl;
		client.Execute(query);
	}
};

}
