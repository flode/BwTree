#include <unordered_map>
#include "bwtree.hpp"
#include <cassert>
#include <algorithm>

namespace BwTree {

    template<typename Key, typename Data>
    Data *Tree<Key, Data>::search(Key key) {
        PID pid;
        Node<Key, Data> *startNode;
        Node<Key, Data> *dataNode;
        std::tie(pid, startNode, dataNode) = findDataPage(key);
        if (dataNode == nullptr) {
            return nullptr;
        }
        switch (dataNode->type) {
            case PageType::leaf: {
                auto node1 = static_cast<Leaf<Key, Data> *>(dataNode);
                auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, 0, key);
                if (res == std::numeric_limits<std::size_t>::max()) {
                    return nullptr;
                } else {
                    if (std::get<0>(node1->records[res]) == key) {
                        return std::get<1>(node1->records[res]);
                    }
                }
            }
            case PageType::deltaInsert: {
                auto node1 = static_cast<DeltaInsert<Key, Data> *>(dataNode);
                if (std::get<0>(node1->record) == key) {
                    return std::get<1>(node1->record);
                }
            }
            case PageType::deltaDelete:
                assert(false);//shouldn't occur here
            default: {
                assert(false);
            }
        }
        assert(false); //here a result should always be found
    }


    template<typename Key, typename Data>
    template<typename T>
    std::size_t Tree<Key, Data>::binarySearch(T array, std::size_t length, std::size_t tupleIndex, Key key) {
        if (length == 0) {
            return std::numeric_limits<std::size_t>::max();
        }
        std::size_t curdif = length >> 1;
        std::size_t i = curdif;
        std::size_t leftInterval = 0, rightInterval = length - 1;
        std::size_t tmpCounterSafe = 0;
        while (leftInterval != rightInterval) {
            curdif >>= 1;
            Key curKey = std::get<0>(array[i]);
            if (curKey == key) {
                return i;
            } else if (curKey < key) {
                if (i == rightInterval) {
                    break;
                }
                leftInterval = i + 1;
                if (curdif == 0) {
                    ++i;
                } else {
                    i += curdif;
                }
            } else {
                if (i == leftInterval) {
                    break;
                }
                rightInterval = i - 1;
                if (curdif == 0) {
                    --i;
                } else {
                    i -= curdif;
                }
            }
            assert(tmpCounterSafe++ < 1000);
        }
        Key curKey = std::get<0>(array[i]);
        if (leftInterval == rightInterval) {
            if ((leftInterval != 0 && rightInterval != length - 1) || curKey == key) {
                return i;
            }
        }
        return std::numeric_limits<std::size_t>::max();
    }


    template<typename Key, typename Data>
    std::tuple<PID, Node<Key, Data> *, Node<Key, Data> *> Tree<Key, Data>::findDataPage(Key key) {
        auto nextPID = root;
        std::size_t debugTMPCheck = 0;
        while (nextPID != std::numeric_limits<PID>::max()) {
            if (debugTMPCheck++ > 100) {
                assert(true);
                std::cout << "test" << std::endl;
            }
            std::size_t pageDepth = 0;
            Node<Key, Data> *startNode = mapping[nextPID];
            auto nextNode = startNode;
            while (nextNode != nullptr) {
                ++pageDepth;
                if (pageDepth == 1000) {//TODO save for later
                    consolidatePage(nextPID);
                }
                switch (nextNode->type) {
                    case PageType::deltaIndex:
                        assert(false);//not implemented
                    case PageType::inner: {
                        auto node1 = static_cast<InnerNode<Key, Data> *>(nextNode);
                        auto res = binarySearch<decltype(node1->nodes)>(node1->nodes, node1->nodeCount, 0, key);
                        if (res == std::numeric_limits<std::size_t>::max()) {
                            return std::make_tuple(nextPID, startNode, nullptr);
                        } else {
                            nextNode = nullptr;
                            nextPID = std::get<1>(node1->nodes[res]);
                        }
                    };
                    case PageType::leaf: {
                        auto node1 = static_cast<Leaf<Key, Data> *>(nextNode);
                        auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, 0, key);
                        if (res == std::numeric_limits<std::size_t>::max()) {
                            return std::make_tuple(nextPID, startNode, nullptr);
                        } else {
                            if (std::get<0>(node1->records[res]) == key) {
                                return std::make_tuple(nextPID, startNode, nextNode);
                            } else {
                                return std::make_tuple(nextPID, startNode, nullptr);
                            }
                        }
                    };
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                        if (std::get<0>(node1->record) == key) {
                            auto &r = node1->record;
                            return std::make_tuple(nextPID, startNode, nextNode);
                        }
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    case PageType::deltaDelete: {
                        auto node1 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                        if (node1->key == key) {
                            return std::make_tuple(nextPID, startNode, nullptr);
                        }
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    case PageType::deltaSplit: {
                        auto node1 = static_cast<DeltaSplit<Key, Data> *>(nextNode);
                        if (key > node1->key) {
                            nextPID = node1->sidelink;
                            nextNode = startNode = nullptr;
                            continue;
                        }
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    default: {
                        assert(false); // not implemented
                    }
                }
                nextNode = nullptr;
            }
        }
        assert(false); // I think this should not happen
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::insert(Key key, Data *record) {
        PID pid;
        Node<Key, Data> *startNode;
        Node<Key, Data> *dataNode;
        std::tie(pid, startNode, dataNode) = findDataPage(key);
        switch (startNode->type) {
            case PageType::inner:
                assert(false); // only leafs  should come to here
            case PageType::deltaInsert:
            case PageType::deltaDelete:
            case PageType::leaf: {
                DeltaInsert<Key, Data> *newNode = CreateDeltaInsert<Key, Data>();
                auto b = *record;
                std::get<0>(newNode->record) = key;
                std::get<1>(newNode->record) = record;
                newNode->origin = startNode;
                if (!mapping[pid].compare_exchange_weak(startNode, newNode)) {
                    ++atomicCollisions;
                    free(newNode);
                    insert(key, record);
                    return;
                }
                return;
            }
        }
        assert(false);
    }


    template<typename Key, typename Data>
    void Tree<Key, Data>::deleteKey(Key key) {
        PID pid;
        Node<Key, Data> *startNode;
        Node<Key, Data> *dataNode;
        std::tie(pid, startNode, dataNode) = findDataPage(key);
        if (dataNode == nullptr) {
            return;
        }
        switch (startNode->type) {
            case PageType::deltaDelete:
            case PageType::deltaInsert:
            case PageType::leaf: {
                DeltaDelete<Key, Data> *newDeleteNode = CreateDeltaDelete<Key, Data>();
                newDeleteNode->key = key;
                newDeleteNode->origin = startNode;
                if (!mapping[pid].compare_exchange_weak(startNode, newDeleteNode)) {
                    ++atomicCollisions;
                    free(newDeleteNode);
                    deleteKey(key);//TODO without recursion
                    return;
                }
                return;
            }
        }
        assert(false);
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::consolidatePage(PID pid) {
        Node<Key, Data> *startNode = mapping[pid];

        Node<Key, Data> *node = startNode;
        std::vector<std::tuple<Key, Data *>> records;
        std::unordered_map<Key, bool> consideredKeys;
        Key stopAtKey;
        bool pageSplit = false;
        PID prev, next;
        while (node != nullptr) {
            switch (node->type) {
                case PageType::leaf: {
                    auto node1 = static_cast<Leaf<Key, Data> *>(node);
                    for (int i = 0; i < node1->recordCount; ++i) {
                        if (consideredKeys.find(std::get<0>(node1->records[i])) == consideredKeys.end()) {
                            records.push_back(node1->records[i]);
                            consideredKeys[std::get<0>(node1->records[i])] = true;
                        }
                    }
                    prev = node1->prev;
                    if (!pageSplit) {
                        next = node1->next;
                    }
                    // found last element in the chain
                    break;
                }
                case PageType::deltaInsert: {
                    auto node1 = static_cast<DeltaInsert<Key, Data> *>(node);
                    if (consideredKeys.find(std::get<0>(node1->record)) == consideredKeys.end()) {
                        records.push_back(node1->record);
                        consideredKeys[std::get<0>(node1->record)] = true;
                    }
                    node = node1->origin;
                    continue;
                }
                case PageType::deltaDelete: {
                    auto node2 = static_cast<DeltaDelete<Key, Data> *>(node);
                    if (consideredKeys.find(node2->key) == consideredKeys.end()) {
                        consideredKeys[node2->key] = true;
                    }
                    node = node2->origin;
                    continue;
                }
                case PageType::deltaSplit: {
                    auto node1 = static_cast<DeltaSplit<Key, Data> *>(node);
                    if (!pageSplit) {
                        pageSplit = true;
                        stopAtKey = node1->key;
                        next = node1->sidelink;
                    }
                    node = node1->origin;
                    continue;
                };
                default: {
                }
            }
            node = nullptr;
        }
        // construct a new node
        auto newNode = CreateLeaf<Key, Data>(records.size());
        std::sort(records.begin(), records.end(), [](const std::tuple<Key, Data *> &t1, const std::tuple<Key, Data *> &t2) {
            return std::get<0>(t1) < std::get<0>(t2);
        });
        //records.data(); // TODO memcopy
        int i = 0;
        for (auto &r : records) {
            newNode->records[i++] = r;
        }
        newNode->next = next;
        newNode->prev = prev;

        if (!mapping[pid].compare_exchange_weak(startNode, newNode)) {
            ++atomicCollisions;
            consolidatePage(pid);
        }
    }

    template<typename Key, typename Data>
    Tree<Key,Data>::~Tree() {
        for (int i = 0; i < mappingNext; ++i) {
            Node<Key,Data> *node = mapping[i];
            while (node != nullptr) {
                switch (node->type) {
                    case PageType::inner: /* fallthrough */
                    case PageType::leaf: {
                        free(node);
                        break;
                    }
                    case PageType::deltaIndex: /* fallthrough */
                    case PageType::deltaDelete: /* fallthrough */
                    case PageType::deltaSplit: /* fallthrough */
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaNode<Key, Data> *>(node);
                        node = node1->origin;
                        free(node1);
                        continue;
                    }
                    default: {
                        assert(false);//all nodes have to be handeled
                    }
                }
                node = nullptr;
            }
        }

    }
}

















