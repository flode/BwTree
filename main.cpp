#include <iostream>
#include <tuple>
#include <random>
#include <unordered_map>
#include <thread>
#include "bwtree.hpp"

using namespace BwTree;

void randomThreadTest() {
    Tree<unsigned long, unsigned> tree;

    std::vector<std::thread> threads;
    constexpr int numberOfThreads = 1;
    constexpr int numberValues = 3;
    for (int i = 0; i < numberOfThreads; ++i) {
        threads.push_back(std::thread([&tree]() {
            std::default_random_engine d;
            std::uniform_int_distribution<unsigned long> rand(0, std::numeric_limits<unsigned long>::max());
            std::vector<unsigned> values;
            for (int i = 0; i < numberValues; ++i) {
                values.push_back(i);//rand(d));
                tree.insert(values[i], &(values[i]));
            }
            for (auto &v : values) {
                auto r = tree.search(v);
                if (r == nullptr || *r!=v) {
                    std::cout << "wrong value!! " << *r << " " << v << std::endl;
                }
            }
        }));
    }

    for (auto &thread : threads) {
        thread.join();
    }
}

int main() {
    randomThreadTest();
    return 0;
    /**
    * Tasks for next week (3.12.2014)
    * - random test pattern
    * - multi thread access
    * - deconstructor
    * - binary search in nodes - OK
    */
    Tree<unsigned long, unsigned> a;
    std::unordered_map<unsigned long, unsigned> map;

    std::vector<unsigned> values;

    std::default_random_engine d;
    std::uniform_int_distribution<int> rand(0, 1000000);
    constexpr int numberValues = 10000;
    for (int i = 0; i < numberValues; ++i) {
        values.push_back(rand(d));
    }

    for (int j = 0; j < numberValues; ++j) {
        a.insert(j, &values[j]);
        map[j] = values[j];
        if (map[j] != *a.search(j)) {
            std::cout << "Wrong result!! result in map:" << map[j] << " result in bw Tree: " << *a.search(j) << std::endl;
        }
    }

    std::vector<unsigned> values2;
    for (int i = 0; i < numberValues; ++i) {
        values2.push_back(rand(d));
    }

    for (int j = 0; j < numberValues; ++j) {
        if (rand(d) < 300000) {
            a.deleteKey(j);
            map.erase(j);
        } else {
            a.insert(j, &values2[j]);
            map[j] = values2[j];
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