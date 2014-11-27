#include <iostream>
#include <tuple>
#include <random>
#include <unordered_map>
#include "bwtree.hpp"

using namespace BwTree;

int main() {
    Tree<unsigned long, unsigned> a;
    std::unordered_map<unsigned long, unsigned> map;

    std::vector<unsigned> values;

    std::default_random_engine d;
    std::uniform_int_distribution<int> rand(0, 1000000);
    for (int i = 0; i < 10000; ++i) {
        values.push_back(rand(d));
    }

    for (int j = 0; j < 10000; ++j) {
        a.insert(j, &values[j]);
        map[j] = values[j];
        if (map[j] != *a.search(j)) {
            std::cout << "Wrong result!! result in map:" << map[j] << " result in bw Tree: " << *a.search(j) << std::endl;
        }
    }

    values.clear();
    for (int i = 0; i < 10000; ++i) {
        values.push_back(rand(d));
    }

    for (int j = 0; j < 10000; ++j) {
        if (rand(d) < 300000) {
            a.deleteKey(j);
            map.erase(j);
        } else {
            a.insert(j, &values[j]);
            map[j] = values[j];
        }
        const auto r1 = map.find(j);
        const auto r2 = a.search(j);
        if ((r1 != map.end() && r2 == nullptr) || (r1 == map.end() && r2 != nullptr) || (r2 != nullptr && map[j] != *r2)) {
            std::cout << "Wrong result!! result in map:" << (r1 == map.end() ? "nullptr" : std::to_string(r1->second)) << " result in bw Tree: " << (r2 == nullptr ? "nullptr" : std::to_string(*r2)) << std::endl;
        }
    }


/*
    unsigned val = 65;
    unsigned val2 = 42;
    unsigned val3 = 67;
    a.insert(6, &val);
    a.insert(2, &val2);
    a.insert(1, &val3);

    auto b = a.search(6);
    std::cout << "result" << *b << std::endl;
    b = a.search(2);
    std::cout << "result" << *b << std::endl;
    b = a.search(1);
    std::cout << "result " << *b << std::endl;

    a.deleteKey(1);
    b = a.search(1);
    std::cout << "result %x" << b << std::endl;
//    std::get<4>();*/
    return 0;
}