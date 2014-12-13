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
                auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, key);
                if (res != 0 && res != std::numeric_limits<std::size_t>::max() && (std::get<0>(node1->records[res - 1]) == key)) {
                    return const_cast<Data *>(std::get<1>(node1->records[res - 1]));
                } else {
                    assert(false); //value should exist at this point
                }
            }
            case PageType::deltaInsert: {
                auto node1 = static_cast<DeltaInsert<Key, Data> *>(dataNode);
                if (std::get<0>(node1->record) == key) {
                    return const_cast<Data *>(std::get<1>(node1->record));
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
    size_t Tree<Key, Data>::binarySearch(T array, std::size_t length, Key key) {
        //std cpp code upper_bound
        std::size_t first = 0;
        std::size_t i;
        std::size_t count, step;
        count = length;
        while (count > 0) {
            i = first;
            step = count / 2;
            i += step;
            if (!(key < std::get<0>(array[i]))) {
                first = ++i;
                count -= step + 1;
            } else {
                count = step;
            }
        }
        if (first >= length) {
            return std::numeric_limits<std::size_t>::max();
        } else {
            return first;
        }
    }


    template<typename Key, typename Data>
    std::tuple<PID, Node<Key, Data> *, Node<Key, Data> *> Tree<Key, Data>::findDataPage(Key key) {
        PID nextPID = root;
        std::size_t debugTMPCheck = 0;
        while (nextPID != NotExistantPID) {
            if (debugTMPCheck++ > 100) {
                assert(true);
                std::cout << "test" << std::endl;
            }
            std::size_t pageDepth = 0;
            Node<Key, Data> *startNode = PIDToNodePtr(nextPID);
            Node<Key, Data> *nextNode = startNode;
            while (nextNode != nullptr) {
                ++pageDepth;
                if (pageDepth == settings.ConsolidateLeafPage) {//TODO save for later
                    consolidateLeafPage(nextPID);
                }
                switch (nextNode->type) {
                    case PageType::deltaIndex:
                        assert(false);//not implemented
                    case PageType::inner: {
                        auto node1 = static_cast<InnerNode<Key, Data> *>(nextNode);
                        auto res = binarySearch<decltype(node1->nodes)>(node1->nodes, node1->nodeCount, key);
                        nextNode = nullptr;
                        nextPID = std::get<1>(node1->nodes[res]);
                        break;
                    };
                    case PageType::leaf: {
                        auto node1 = static_cast<Leaf<Key, Data> *>(nextNode);
                        auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, key);
                        if (res == 0 || res == std::numeric_limits<std::size_t>::max()) {
                            return std::make_tuple(nextPID, startNode, nullptr);
                        } else {
                            if (std::get<0>(node1->records[res - 1]) == key) {
                                return std::make_tuple(nextPID, startNode, nextNode);
                            } else {
                                return std::make_tuple(nextPID, startNode, nullptr);
                            }
                        }
                    };
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                        if (std::get<0>(node1->record) == key) {
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
    void Tree<Key, Data>::insert(Key key, const Data *const record) {
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
            default:
                assert(false); //shouldn't happen
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
    void Tree<Key, Data>::consolidateLeafPage(PID pid) {
        Node<Key, Data> *startNode = mapping[pid];

        Node<Key, Data> *node = startNode;
        std::vector<std::tuple<Key, const Data *>> records;
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
                    assert(false); //shouldn't occur here
                }
            }
            node = nullptr;
        }
        // construct a new node
        auto newNode = CreateLeaf<Key, Data>(records.size());
        auto &a = records[0];
        std::sort(records.begin(), records.end(), [](const std::tuple<Key, const Data *> &t1, const std::tuple<Key, const Data *> &t2) {
            return std::get<0>(t1) < std::get<0>(t2);
        });
        int i = 0;
        for (auto &r : records) {
            newNode->records[i++] = r;
        }
        newNode->next = next;
        newNode->prev = prev;
        Node<Key,Data> *previousNode = startNode;
        if (!mapping[pid].compare_exchange_weak(startNode, newNode)) {
            ++atomicCollisions;
            consolidateLeafPage(pid);
        } else {
            markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    Tree<Key, Data>::~Tree() {
        for (int i = 0; i < mappingNext; ++i) {
            Node<Key, Data> *node = mapping[i];
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

    template<typename Key, typename Data>
    void Tree<Key, Data>::markForDeletion(Node<Key, Data> *node) {
        std::size_t index = deleteNodeNext++;
        if (index == deletedNodes.size())
            assert(false); //too many deleted nodes
        deletedNodes[index] = node;
    }
}

















