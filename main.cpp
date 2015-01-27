#include <iostream>
#include <tuple>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include "bwtree.hpp"

using namespace BwTree;

void executeBwTree(const std::size_t numberOfThreads, const std::vector<unsigned long long> &values, const std::vector<unsigned long long> &initial_values, const std::size_t operations, const unsigned percentRead, BwTree::Tree<unsigned long long, unsigned long long> &tree);

void testBwTree() {
    std::cout << "threads, operations,percent read operations, settings, time in ms, exchange collisions, successful leaf consolidation, failed leaf consolidation, leaf consolidation time avg, successful leaf split, failed leaf split,"
            "leaf split time avg, successful inner consolidation, failed inner consolidation, inner consolidation time avg, successful inner split, failed innersplit, inner split time avg" << std::endl;

    std::vector<std::size_t> numberValuesChoice{{1000, 10000, 100000, 1000000, 10000000}};
    std::default_random_engine d;

    std::size_t initial_values_count = 1000000;
    std::uniform_int_distribution<unsigned long long> rand(1, initial_values_count * 2);
    std::vector<unsigned long long> initial_values(initial_values_count);
    std::unordered_set<unsigned long long> keys;
    for (std::size_t i = 0; i < initial_values_count; ++i) {
        unsigned long long val;
        do {
            val = rand(d);
        } while (keys.find(val) != keys.end());
        keys.emplace(val);
        initial_values[i] = val;
    }

    for (auto &numberValues : numberValuesChoice) {
        for (int numberOfThreads = 1; numberOfThreads <= 4; ++numberOfThreads) {

            std::uniform_int_distribution<unsigned long long> rand(1, numberValues * 2);
            std::vector<unsigned long long> values(numberValues);
            std::unordered_set<unsigned long long> keys;
            for (std::size_t i = 0; i < numberValues; ++i) {
                unsigned long long val;
                do {
                    val = rand(d);
                } while (keys.find(val) != keys.end());
                keys.emplace(val);
                values[i] = val;
            }


            std::vector<BwTree::Settings> settingsList{{
                    BwTree::Settings("single", 200, {{100}}, 5, {{5}}),
                    BwTree::Settings("multiple consolidate", 200, {{100}}, 5, {{2, 3, 4}}),
                    BwTree::Settings("multiple split and consolidate", 200, {{50, 100, 200}}, 5, {{2, 3, 4}})
            }};
            for (auto &settings : settingsList) {
                std::vector<std::tuple<unsigned long, int>> operationsList{{std::make_tuple(values.size(),66),std::make_tuple(values.size(),50),std::make_tuple(values.size(),33)}};
                for (const auto &operationsTuple : operationsList) {
                    Tree<unsigned long long, unsigned long long> tree(settings);
                    executeBwTree(3, initial_values, initial_values, initial_values_count, 0, tree);

                    auto starttime = std::chrono::system_clock::now();

                    const std::size_t operations = std::get<0>(operationsTuple);
                    const std::size_t percentRead = std::get<1>(operationsTuple);
                    std::cout << numberOfThreads << "," << operations << "," << percentRead << "," << settings.getName() << ",";

                    executeBwTree(numberOfThreads, values, initial_values, operations, percentRead, tree);

                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - starttime);
                    std::cout << duration.count() << ", ";


                    std::cout << tree.getAtomicCollisions() << ",";
                    std::cout << tree.getSuccessfulLeafConsolidate() << ",";
                    std::cout << tree.getFailedLeafConsolidate() << ",";
                    if (tree.getSuccessfulLeafConsolidate() != 0)std::cout << tree.getTimeForLeafConsolidation() / tree.getSuccessfulLeafConsolidate();
                    std::cout << ",";
                    std::cout << tree.getSuccessfulLeafSplit() << ",";
                    std::cout << tree.getFailedLeafSplit() << ",";
                    if (tree.getSuccessfulLeafSplit() != 0)std::cout << tree.getTimeForLeafSplit() / tree.getSuccessfulLeafSplit();
                    std::cout << ",";
                    std::cout << tree.getSuccessfulInnerConsolidate() << ",";
                    std::cout << tree.getFailedInnerConsolidate() << ",";
                    if (tree.getSuccessfulInnerConsolidate() != 0)std::cout << tree.getTimeForInnerConsolidation() / tree.getSuccessfulInnerConsolidate();
                    std::cout << ",";
                    std::cout << tree.getSuccessfulInnerSplit() << ",";
                    std::cout << tree.getFailedInnerSplit() << ",";
                    if (tree.getSuccessfulInnerSplit() != 0)std::cout << tree.getTimeForInnerSplit() / tree.getSuccessfulInnerSplit();
                    std::cout << ",";
                    std::cout << std::endl;
                }
            }
        };

    }
};

void executeBwTree(const std::size_t numberOfThreads, const std::vector<unsigned long long> &values, const std::vector<unsigned long long> &initial_values, const std::size_t operations, const unsigned percentRead, BwTree::Tree<unsigned long long, unsigned long long> &tree) {
    std::default_random_engine d;
    std::uniform_int_distribution<unsigned> rand(1, 100);

    std::vector<std::thread> threads;
    std::atomic<int> c{0};
    std::size_t start = 0;
    std::size_t delta = values.size() / numberOfThreads;
    std::size_t startOps = 0;
    std::size_t deltaOps = operations / numberOfThreads;
    for (std::size_t i = 0; i < numberOfThreads; ++i) {
        threads.push_back(std::thread([&tree, &values, &initial_values, start, delta, deltaOps, &rand, &d, &percentRead]() {
            std::uniform_int_distribution<std::size_t> randCoin(1, 2);
            std::size_t writeOperations = 0;
            for (std::size_t i = 0; i < deltaOps; ++i) {
                if ((rand(d) < percentRead) || writeOperations == delta) {
                    if (writeOperations != 0 && randCoin(d) == 1) {
                        std::uniform_int_distribution<std::size_t> randRead(0, writeOperations);
                        tree.search(values[start + randRead(d)]);
                    } else {
                        std::uniform_int_distribution<std::size_t> randRead(0, initial_values.size());
                        tree.search(initial_values[randRead(d)]);
                    }
                } else {
                    tree.insert(values[start + writeOperations], &values[start + writeOperations]);
                    writeOperations++;
                }
            }
            tree.threadFinishedWithTree();
        }));
        start += delta;
        startOps += deltaOps;
    }

    for (auto &thread : threads) {
        thread.join();
    }
}


void randomThreadTest() {
    std::vector<std::thread> threads;
    constexpr int numberOfThreads = 4;
    constexpr int numberValues = 5000000;


    std::default_random_engine d;
    std::uniform_int_distribution<unsigned long long> rand(1, 99999999);
    std::vector<unsigned long long> values(numberValues);
    std::unordered_set<unsigned long long> keys;
    for (std::size_t i = 0; i < numberValues; ++i) {
        unsigned long long val;
        do {
            val = rand(d);
        } while (keys.find(val) != keys.end());
        keys.emplace(val);
        values[i] = val;
    }


    auto starttime = std::chrono::system_clock::now();

    std::vector<std::size_t> splitInner{{100}};
    std::vector<std::size_t> consolidateInner{{2, 3, 4}};
    BwTree::Settings settings("default", 200, splitInner, 5, consolidateInner);
    Tree<unsigned long long, unsigned long long> tree(settings);

    std::size_t start = 0;
    std::size_t delta = numberValues / numberOfThreads;
    const auto &t_values = values;
    for (int i = 0; i < numberOfThreads; ++i) {
        threads.push_back(std::thread([&tree, &t_values, start, delta]() {
            for (int i = start; i < start + delta; ++i) {
                tree.insert(t_values[i], &(t_values[i]));
            }
            for (int i = start; i < start + delta; ++i) {
                auto &v = t_values[i];
                auto r = tree.search(v);
                if (r == nullptr || *r != v) {
                    std::cout << "wrong value inner!! " << (r == nullptr ? "NULLPTR" : std::to_string(*r)) << " " << v << std::endl;
                }
            }
        }));
        start += delta;
    }

    for (auto &thread : threads) {
        thread.join();
    }
    for (auto &v : values) {
        auto r = tree.search(v);
        if (r == nullptr || *r != v) {
            std::cout << "wrong value!! " << (r == nullptr ? "NULLPTR" : std::to_string(*r)) << " " << v << std::endl;
        }
    }

    std::cout << "    " << c << std::endl;
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - starttime);
    std::cout << "Elapsed time in ms: " << duration.count() << std::endl;


    std::cout << "exchange collisions: " << tree.getAtomicCollisions() << std::endl;
    std::cout << "successful leaf consolidation: " << tree.getSuccessfulLeafConsolidate() << std::endl;
    std::cout << "failed leaf consolidation: " << tree.getFailedLeafConsolidate() << std::endl;
    if (tree.getSuccessfulLeafConsolidate() != 0)std::cout << "leaf consolidation time avg: " << tree.getTimeForLeafConsolidation() / tree.getSuccessfulLeafConsolidate() << std::endl;
    std::cout << "successful leaf split: " << tree.getSuccessfulLeafSplit() << std::endl;
    std::cout << "failed leaf split: " << tree.getFailedLeafSplit() << std::endl;
    if (tree.getSuccessfulLeafSplit() != 0)std::cout << "leaf split time avg: " << tree.getTimeForLeafSplit() / tree.getSuccessfulLeafSplit() << std::endl;
    std::cout << "successful inner consolidation: " << tree.getSuccessfulInnerConsolidate() << std::endl;
    std::cout << "failed inner consolidation: " << tree.getFailedInnerConsolidate() << std::endl;
    if (tree.getSuccessfulInnerConsolidate() != 0)std::cout << "inner consolidation time avg: " << tree.getTimeForInnerConsolidation() / tree.getSuccessfulInnerConsolidate() << std::endl;
    std::cout << "successful inner split: " << tree.getSuccessfulInnerSplit() << std::endl;
    std::cout << "failed innersplit: " << tree.getFailedInnerSplit() << std::endl;
    if (tree.getSuccessfulInnerSplit() != 0)std::cout << "inner split time avg: " << tree.getTimeForInnerSplit() / tree.getSuccessfulInnerSplit() << std::endl;
}

int main() {
    testBwTree();
    //randomThreadTest();
    return 0;
    /**
    * Tasks for next week (3.12.2014)
    * - random test pattern - OK
    * - multi thread access - OK
    * - deconstructor - OK
    * - binary search in nodes - OK
    */
    /**
    * Tasks for next week (11.12.2014)
    * - fix random bugs that occur with threads :( - OK!
    * - check that all B+ properties hold, linked, additional pointer
    * - manuel split of nodes
    * - consolidate inner nodes as well
    */
    /**
    * Tasks for until 6/january
    * - completet split page, propagate split key to parent
    * - consolidate inner nodes
    * - different consolidation, dependant on depth in the tree
    * - generate a lot of numbers
    */
    std::vector<std::size_t> splitInner{{100}};
    std::vector<std::size_t> consolidateInner{{4}};
    BwTree::Settings settings("default", 200, splitInner, 5, consolidateInner);
    Tree<unsigned long, unsigned> a(settings);
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