#pragma once

#include <string>
#include <fstream>
#include <iostream>
#include <exception>

#include <clickhouse/client.h>

namespace chfuzz {

class ClientContext {
private:
	std::ofstream outf;

public:
	clickhouse::Client client;

	ClientContext(const clickhouse::ClientOptions opt) : client(opt),
														 outf("/tmp/out.sql", std::ios::out | std::ios::trunc) {}

	bool LogQuery(const std::string &query) {
		try {
			std::cout << "Running query: " << query << std::endl;
			outf << query << std::endl;
			client.Execute(query);
			return true;
		} catch (const std::exception & e) {
			std::cout << "Got exception " << e.what() << std::endl;
			if (std::strstr(e.what(), "Broken pipe")) {
				std::cerr << "Server crashed, exiting" << std::endl;
				std::exit(1);
			}
		}
		return false;
	}
};

}
