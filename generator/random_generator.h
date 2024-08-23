#pragma once

#include <cstdint>
#include <random>
#include <map>

namespace chfuzz {

class RandomGenerator {
private:
	std::uniform_int_distribution<uint32_t> dist1, dist2, dist3, dist4, uints;

public:
	std::mt19937 gen;

	RandomGenerator(const uint32_t seed) : dist1(1, 10), dist2(1, 100), dist3(1, 1000), dist4(1, 2),
										   uints(std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max()) {
		std::random_device rd;
		gen = std::mt19937(seed ? seed : rd());
	}

	const uint32_t NextSmallNumber() {
		return dist1(gen);
	}

	const uint32_t NextMediumNumber() {
		return dist2(gen);
	}

	const uint32_t NextLargeNumber() {
		return dist3(gen);
	}

	const uint32_t NextRandomUInt32() {
		return uints(gen);
	}

	const bool NextBool() {
		return dist4(gen) == 2;
	}

	template <typename T>
	const T& PickRandomlyFromVector(const std::vector<T> &vals) {
		std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
		return vals[d(gen)];
	}

	template <typename K, typename V>
	const V& PickKeyRandomlyFromMap(const std::map<K,V> &vals) {
		std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
		auto it = vals.begin();
		std::advance(it, d(gen));
		return it->second;
	}
};

}
