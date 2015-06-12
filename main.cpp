#include <iostream>
#include <tuple>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include "bwtree.hpp"
#include "main.hpp"

using namespace BwTree;

template<typename Key>
void testBwTreeNew(const std::size_t count) {
    auto settings = BwTree::Settings("8,8,3,3", 8, {{8}}, 3, {{3}});
    Tree<Key, Key> tree(settings);
    std::vector<Key> values(count);
    for (std::size_t i = 0; i < count; ++i) {
        values.at(i) = i;
        tree.insert(values.at(i), &values.at(i));
    }
    tree.deleteKey(values.at(0));
    tree.deleteKey(values.at(1));
    tree.deleteKey(values.at(2));
    tree.deleteKey(values.at(3));
    tree.deleteKey(values.at(4));
    tree.deleteKey(values.at(70));
    for (std::size_t i = 0; i < count; ++i) {
        auto *val = tree.search(values.at(i));
        if (val == nullptr || *val != values.at(i)) {
            std::cout << "error val " << (val == nullptr ? -1 : *val) << " expected " << values.at(i) << std::endl;
        }
    }
}

template<typename Key>
void testBwTree() {
    std::cout << "threads, operations,percent read operations, settings split leaf, settings split inner, settings delta, settings delta inner, time in ms, operations per s, exchange collisions, successful leaf consolidation, failed leaf consolidation, successful leaf split, failed leaf split,"
            "successful inner consolidation, failed inner consolidation, successful inner split, failed innersplit" << std::endl;
    std::default_random_engine d;
    std::size_t initial_values_count = 1000000;
    std::uniform_int_distribution<Key> rand(1, initial_values_count * 2);
    std::vector<Key> initial_values(initial_values_count);
    {
        std::unordered_set<Key> keys;
        for (std::size_t i = 0; i < initial_values_count; ++i) {
            Key val;
            do {
                val = rand(d);
            } while (keys.find(val) != keys.end());
            keys.emplace(val);
            initial_values[i] = val;
        }
    }
    std::vector<std::size_t> numberValuesChoice{{1000000,10000000, 42000000}};
    for (auto &numberValues : numberValuesChoice) {
        for (int numberOfThreads = 1; numberOfThreads <= 8; ++numberOfThreads) {
            std::uniform_int_distribution<Key> rand(1, numberValues * 2);
            std::vector<Key> values(numberValues);

            {
                std::unordered_set<Key> keys;
                for (std::size_t i = 0; i < numberValues; ++i) {
                    unsigned long long val;
                    do {
                        val = rand(d);
                    } while (keys.find(val) != keys.end());
                    keys.emplace(val);
                    values[i] = val;
                }
            }


            std::vector<BwTree::Settings> settingsList{{
/*                    BwTree::Settings("single", 200, {{100}}, 5, {{5}}),
                    BwTree::Settings("single", 200, {{100}}, 7, {{6}}),
                    BwTree::Settings("single", 200, {{100}}, 7, {{5,6}}),
                    BwTree::Settings("single", 200, {{100}}, 7, {{4,5,6}}),
                    BwTree::Settings("single", 200, {{100}}, 7, {{5,6,7}}),

                    BwTree::Settings("single", 200, {{100}}, 5, {{7}}),*/
/*                    BwTree::Settings("200, 100, 4", 200, {{100}}, 4, {{4}}),
                    BwTree::Settings("200, 100, 5", 200, {{100}}, 5, {{5}}),
                    BwTree::Settings("200, 100, 6", 200, {{100}}, 6, {{6}}),
                    BwTree::Settings("200, 100, 7", 200, {{100}}, 7, {{7}}),
                    BwTree::Settings("200, 100, 8", 200, {{100}}, 8, {{8}}),


                    BwTree::Settings("50, 100, 7", 50, {{100}}, 7, {{7}}),
                    BwTree::Settings("100, 100, 7", 100, {{100}}, 7, {{7}}),
                    BwTree::Settings("200, 100, 7", 200, {{100}}, 7, {{7}}),
                    BwTree::Settings("300, 100, 7", 300, {{100}}, 7, {{7}}),
                    BwTree::Settings("400, 100, 7", 400, {{100}}, 7, {{7}}),

                    BwTree::Settings("50, 200, 7", 50, {{200}}, 7, {{7}}),
                    BwTree::Settings("100, 200, 7", 100, {{200}}, 7, {{7}}),
                    BwTree::Settings("200, 200, 7", 200, {{200}}, 7, {{7}}),
                    BwTree::Settings("300, 200, 7", 300, {{200}}, 7, {{7}}),
                    BwTree::Settings("400, 200, 7", 400, {{200}}, 7, {{7}}),
		    */
/*                    BwTree::Settings("400, 200, 7, 7", 400, {{200}}, 7, {{7}}),
                    BwTree::Settings("800, 200, 7, 7", 800, {{200}}, 7, {{7}}),
                    BwTree::Settings("8000, 200, 7, 7", 8000, {{200}}, 7, {{7}}),*/


                    BwTree::Settings("400, 200, 7, 7", 400, {{200}}, 7, {{7}}),
                    BwTree::Settings("400, 400, 7, 7", 400, {{400}}, 7, {{7}}),
                    BwTree::Settings("400, 4000, 7, 7", 400, {{4000}}, 7, {{7}}),

                    BwTree::Settings("400, 200, 7, 27", 400, {{200}}, 7, {{2, 7}}),
                    BwTree::Settings("400, 200, 7, 37", 400, {{200}}, 7, {{3, 7}}),
                    BwTree::Settings("400, 200, 7, 347", 400, {{200}}, 7, {{3, 4, 7}}),

                    BwTree::Settings("400, 200, 2, 7", 400, {{200}}, 7, {{7}}),
                    BwTree::Settings("400, 200, 7, 7", 400, {{200}}, 7, {{7}}),
                    BwTree::Settings("400, 200, 14, 7", 400, {{200}}, 7, {{7}}),

                    //BwTree::Settings("single", 200, {{100}}, 8, {{8}}),



                    //BwTree::Settings("multiple consolidate", 200, {{100}}, 5, {{2, 3, 4}}),
                    //BwTree::Settings("multiple split and consolidate", 200, {{50, 100, 200}}, 5, {{2, 3, 4}})
            }};

            for (auto &settings : settingsList) {
                std::vector<std::tuple<std::size_t, int>> operationsList{{std::make_tuple(values.size(), 83), std::make_tuple(values.size(), 0), std::make_tuple(values.size(), 100)}};
                for (const auto &operationsTuple : operationsList) {
                    Tree<Key, Key> tree(settings);

                    createBwTreeCommands(1, initial_values, initial_values, initial_values_count, 0, tree, false);
                    const std::size_t operations = std::get<0>(operationsTuple);
                    const std::size_t percentRead = std::get<1>(operationsTuple);
                    auto duration = createBwTreeCommands(numberOfThreads, values, initial_values, operations, percentRead, tree, false);

                    std::cout << numberOfThreads << "," << operations << "," << percentRead << "," << settings.getName() << ",";

                    std::cout << duration.count() << ", ";
                    std::cout << (duration.count() > 0 ? (operations / duration.count() * 1000) : 0) << ", ";

                    std::cout << tree.getAtomicCollisions() << ",";
                    std::cout << tree.getSuccessfulLeafConsolidate() << ",";
                    std::cout << tree.getFailedLeafConsolidate() << ",";
                    std::cout << tree.getSuccessfulLeafSplit() << ",";
                    std::cout << tree.getFailedLeafSplit() << ",";
                    std::cout << tree.getSuccessfulInnerConsolidate() << ",";
                    std::cout << tree.getFailedInnerConsolidate() << ",";
                    std::cout << tree.getSuccessfulInnerSplit() << ",";
                    std::cout << tree.getFailedInnerSplit() << ",";
                    std::cout << std::endl;
                }
            }
        };

    }
};

template<typename Key>
std::chrono::milliseconds createBwTreeCommands(const std::size_t numberOfThreads, const std::vector<Key> &values, const std::vector<Key> &initial_values, const std::size_t operations, const unsigned percentRead, BwTree::Tree<Key, Key> &tree, bool block) {
    std::default_random_engine d;
    std::uniform_int_distribution<unsigned> rand(1, 100);

    std::size_t start = 0;
    std::size_t delta = values.size() / numberOfThreads;
    std::size_t startOps = 0;
    std::size_t deltaOps = operations / numberOfThreads;
    std::vector<std::vector<BwTreeCommand<Key, Key>>> commands(numberOfThreads);
    for (std::size_t thread_i = 0; thread_i < numberOfThreads; ++thread_i) {
        std::uniform_int_distribution<std::size_t> randCoin(1, 2);
        std::size_t writeOperations = 0;
        std::vector<BwTreeCommand<Key, Key>> &cmds = commands[thread_i];
        for (std::size_t op_i = 0; op_i < deltaOps; ++op_i) {
            if ((rand(d) < percentRead) || writeOperations == delta) {
                if (writeOperations != 0 && randCoin(d) == 1) {
                    std::uniform_int_distribution<std::size_t> randRead(0, writeOperations);
                    cmds.push_back(BwTreeCommand<Key, Key>(BwTreeCommandType::search, values[start + randRead(d)], nullptr));
                } else {
                    std::uniform_int_distribution<std::size_t> randRead(0, initial_values.size());
                    cmds.push_back(BwTreeCommand<Key, Key>(BwTreeCommandType::search, initial_values[randRead(d)], nullptr));
                }
            } else {
                cmds.push_back(BwTreeCommand<Key, Key>(BwTreeCommandType::insert, values[start + writeOperations], &values[start + writeOperations]));
                writeOperations++;
            }
        }
        start += delta;
        startOps += deltaOps;
    }

    if (block) BLOCK();
    auto starttime = std::chrono::system_clock::now();
    executeBwTreeCommands(commands, tree);
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - starttime);
    if (block) BLOCK();

    return duration;
}

template<typename Key>
void executeBwTreeCommands(const std::vector<std::vector<BwTreeCommand<Key, Key>>> &commands, BwTree::Tree<Key, Key> &tree) {
    std::vector<std::thread> threads;
    for (auto &cmds : commands) {
        threads.push_back(std::thread([&tree, &cmds]() {
            for (auto &command : cmds) {
                switch (command.type) {
                    case BwTreeCommandType::insert:
                        tree.insert(command.key, command.data);
                        break;
                    case BwTreeCommandType::search:
                        tree.search(command.key);
                        break;
                }
            }
            tree.threadFinishedWithTree();
        }));
    }

    for (auto &thread : threads) {
        thread.join();
    }
}

int main() {
//    testBwTreeNew<unsigned long long>(293);
//    for (std::size_t i = 20; i < 300; ++i) {
//        std::cout << i << std::endl;
//        testBwTreeNew<unsigned long long>(i);
//    }
//    return EXIT_SUCCESS;
    testBwTree<unsigned long long>();
    return EXIT_SUCCESS;
}
