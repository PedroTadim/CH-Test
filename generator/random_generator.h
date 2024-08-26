#pragma once

#include <cstdint>
#include <random>
#include <map>
#include <chrono>

namespace chfuzz {

static inline std::tm
make_tm(int year, int month, int day) {
	std::tm tm = {0};
	tm.tm_year = year - 1900; // years count from 1900
	tm.tm_mon = month - 1;    // months count from January=0
	tm.tm_mday = day;         // days count from 1
	return tm;
}

class RandomGenerator {
private:
	std::uniform_int_distribution<int8_t> ints8;

	std::uniform_int_distribution<uint8_t> uints8;

	std::uniform_int_distribution<int16_t> ints16;

	std::uniform_int_distribution<uint16_t> uints16;

	std::uniform_int_distribution<int32_t> ints32;

	std::uniform_int_distribution<uint32_t> uints32, dist1, dist2, dist3, dist4, digits, json_cols;

	std::uniform_int_distribution<int64_t> ints64;

	std::uniform_int_distribution<uint64_t> uints64;

	std::uniform_real_distribution<double> doubles;

	std::uniform_int_distribution<uint64_t> dates, datetime, datetime64;

	const int seconds_per_day = 60*60*24;
	std::time_t basetime1, basetime2, basetime3;

	std::vector<std::string> common_english{"is","was","are","be","have","had","were","can","said","use",
		"do","will","would","make","like","has","look","write","go","see",
		"could","been","call","am","find","did","get","come","made","may",
		"take","know","live","give","think","say","help","tell","follow","came",
		"want","show","set","put","does","must","ask","went","read","need",
		"move","try","change","play","spell","found","study","learn","should","add",
		"keep","start","thought","saw","turn","might","close","seem","open","begin",
		"got","run","walk","began","grow","took","carry","hear","stop","miss", "eat",
		"watch","let","cut","talk","being","leave", "water","day","part","sound","work",
		"place","year","back","thing","name", "sentence","man","line","boy"};

	std::vector<std::string> common_chinese{"认识你很高兴", "美国", "叫", "名字", "你们", "日本", "哪国人",
		"爸爸", "兄弟姐妹", "漂亮", "照片"};

public:
	std::mt19937 gen;

	RandomGenerator(const uint32_t seed) : dist1(1, 10), dist2(1, 100), dist3(1, 1000), dist4(1, 2),
										   ints8(std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max()),
										   uints8(std::numeric_limits<uint8_t>::min(), std::numeric_limits<uint8_t>::max()),
										   ints16(std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max()),
										   uints16(std::numeric_limits<uint16_t>::min(), std::numeric_limits<uint16_t>::max()),
										   ints32(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()),
										   uints32(std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max()),
										   ints64(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()),
										   uints64(std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max()),
										   doubles(std::numeric_limits<double>::min(), std::numeric_limits<double>::max()),
										   json_cols(0, 5),
										   digits(0, 9) {
		std::random_device rd;
		gen = std::mt19937(seed ? seed : rd());

		std::tm tm1 = make_tm(1970,1,1), tm2 = make_tm(2149,6,6),
				tm3 = make_tm(1900,1,1), tm4 = make_tm(2299,12,31),
				tm5 = make_tm(2106,02,07);
		basetime1 = std::mktime(&tm1);
		basetime2 = std::mktime(&tm2);
		basetime3 = std::mktime(&tm3);

		const std::time_t time2 = std::mktime(&tm2),
						  time3 = std::mktime(&tm3),
						  time4 = std::mktime(&tm4),
						  difference1 = (basetime1 - time2) / seconds_per_day,
						  difference2 = (basetime2 - time3) / seconds_per_day,
						  difference3 = (basetime3 - time4) / seconds_per_day;

		dates = std::uniform_int_distribution<uint64_t>(0, difference1);
		datetime = std::uniform_int_distribution<uint64_t>(0, difference2);
		datetime64 = std::uniform_int_distribution<uint64_t>(0, difference3);
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

	const uint8_t NextRandomUInt8() {
		return uints8(gen);
	}

	const uint8_t NextRandomInt8() {
		return ints8(gen);
	}

	const uint16_t NextRandomUInt16() {
		return uints16(gen);
	}

	const uint16_t NextRandomInt16() {
		return ints16(gen);
	}

	const uint32_t NextRandomUInt32() {
		return uints32(gen);
	}

	const uint32_t NextRandomInt32() {
		return ints32(gen);
	}

	const uint32_t NextRandomUInt64() {
		return uints64(gen);
	}

	const uint32_t NextRandomInt64() {
		return ints64(gen);
	}

	const uint32_t NextDigit() {
		return digits(gen);
	}

	const uint32_t NextJsonCol() {
		return json_cols(gen);
	}

	const double NextRandomDouble() {
		return doubles(gen);
	}

	const bool NextBool() {
		return dist4(gen) == 2;
	}

	const uint64_t NextDate32() {
		const std::time_t time_conv = (std::time_t) dates(gen),
						  difference = (basetime1 - time_conv) / seconds_per_day;

		return (uint64_t) difference;
	}

	const uint64_t NextDateTime() {
		const std::time_t time_conv = (std::time_t) datetime(gen),
						  difference = (basetime2 - time_conv) / seconds_per_day;

		return (uint64_t) difference;
	}

	const uint64_t NextDateTime64() {
		const std::time_t time_conv = (std::time_t) datetime64(gen),
						  difference = (basetime3 - time_conv) / seconds_per_day;

		return (uint64_t) difference;
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

	void NextString(std::string &ret, const uint32_t limit) {
		const std::string &pick = PickRandomlyFromVector(this->NextBool() ? common_english : common_chinese);

		if (pick.length() < limit) {
			ret += pick;
			return;
		}
		ret += "a";
	}
};

}
