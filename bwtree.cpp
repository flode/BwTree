#ifndef BWTREE_CPP
#define BWTREE_CPP

#include <unordered_map>
#include "bwtree.hpp"
#include <cassert>
#include <algorithm>

namespace BwTree {

    template<typename Key, typename Data>
    Data *Tree<Key, Data>::search(Key key) {
        FindDataPageResult<Key, Data> res = findDataPage(key);
        Data *returnValue;
        if (res.dataNode == nullptr) {
            returnValue = nullptr;
        } else {
            returnValue = const_cast<Data *>(res.data);
        }
        if (res.needSplitPage.size() > 0) {
            splitLeafPage(std::move(res.parentNodes));
        } else if (res.needConsolidatePage.size() > 0) {
            consolidatePage(std::move(res.parentNodes));
        }
        return returnValue;
    }

    template<typename Key, typename Data>
    template<typename T>
    size_t Tree<Key, Data>::binarySearch(T array, std::size_t length, Key key) {
        //std cpp code lower_bound
        std::size_t first = 0;
        std::size_t i;
        std::size_t count, step;
        count = length;
        while (count > 0) {
            i = first;
            step = count / 2;
            i += step;
            if (std::get<0>(array[i]) < key) {
                first = ++i;
                count -= step + 1;
            } else {
                count = step;
            }
        }
        return first;
    }

    template<typename Key, typename Data>
    FindDataPageResult<Key, Data> Tree<Key, Data>::findDataPage(Key key) {
        PID nextPID = root;
        std::size_t debugTMPCheck = 0;
        std::stack<PID> needConsolidatePage;
        std::stack<PID> needSplitPage;
        std::stack<PID> parentNodes;
        while (nextPID != NotExistantPID) {
            if (debugTMPCheck++ > 100) {
                assert(true);
                std::cout << "test" << std::endl;
            }
            std::size_t pageDepth = 0;
            parentNodes.push(nextPID);
            Node<Key, Data> *startNode = PIDToNodePtr(nextPID);
            Node<Key, Data> *nextNode = startNode;
            while (nextNode != nullptr) {
                ++pageDepth;
                if (pageDepth == settings.ConsolidateLeafPage && needConsolidatePage.size() == 0) {//TODO save for later
                    needConsolidatePage = parentNodes;
                }
                switch (nextNode->type) {
                    case PageType::deltaIndex:
                        assert(false);//not implemented
                    case PageType::inner: {
                        auto node1 = static_cast<InnerNode<Key, Data> *>(nextNode);
                        auto res = binarySearch<decltype(node1->nodes)>(node1->nodes, node1->nodeCount, key);
                        nextPID = std::get<1>(node1->nodes[res]);
                        nextNode = nullptr;
                        break;
                    };
                    case PageType::leaf: {
                        auto node1 = static_cast<Leaf<Key, Data> *>(nextNode);
                        if (node1->recordCount > settings.SplitLeafPage && needSplitPage.size() == 0) {
                            needSplitPage = parentNodes;
                        }
                        auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, key);
                        if (node1->recordCount > res && std::get<0>(node1->records[res]) == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key, std::get<1>(node1->records[res]), needConsolidatePage, needSplitPage, parentNodes);
                        } else {
                            if (res == node1->recordCount && node1->next != NotExistantPID) {
                                nextPID = node1->next;
                                nextNode = nullptr;
                                continue;
                            }
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nullptr, needConsolidatePage, needSplitPage, parentNodes);
                        }
                    };
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                        if (std::get<0>(node1->record) == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key, std::get<1>(node1->record), needConsolidatePage, needSplitPage, parentNodes);
                        }
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    case PageType::deltaDelete: {
                        auto node1 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                        if (node1->key == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nullptr, needConsolidatePage, needSplitPage, parentNodes);
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
        FindDataPageResult<Key, Data> res = findDataPage(key);
        switch (res.startNode->type) {
            case PageType::inner:
                assert(false); // only leafs  should come to here
            case PageType::deltaInsert:
            case PageType::deltaDelete:
            case PageType::deltaSplit:
            case PageType::leaf: {
                DeltaInsert<Key, Data> *newNode = CreateDeltaInsert<Key, Data>(res.startNode, std::make_tuple(key, record));
                if (!mapping[res.pid].compare_exchange_weak(res.startNode, newNode)) {
                    ++atomicCollisions;
                    free(newNode);
                    insert(key, record);
                    return;
                } else {
                    if (res.needSplitPage.size() > 0) {
                        splitLeafPage(std::move(res.parentNodes));
                    } else if (res.needConsolidatePage.size() > 0) {
                        consolidatePage(std::move(res.parentNodes));
                    }
                    return;
                }
            }
            default:
                assert(false); //shouldn't happen
        }
        assert(false);
    }


    template<typename Key, typename Data>
    void Tree<Key, Data>::deleteKey(Key key) {
        FindDataPageResult<Key, Data> res = findDataPage(key);
        if (res.dataNode == nullptr) {
            return;
        }
        switch (res.startNode->type) {
            case PageType::deltaDelete:
            case PageType::deltaInsert:
            case PageType::leaf: {
                DeltaDelete<Key, Data> *newDeleteNode = CreateDeltaDelete<Key, Data>(res.startNode, key);
                if (!mapping[res.pid].compare_exchange_weak(res.startNode, newDeleteNode)) {
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
    void Tree<Key, Data>::splitLeafPage(std::stack<PID> &&stack) {
        //TODO how to prevent double split?
        PID pid = stack.top();
        stack.pop();
        Node<Key, Data> *startNode = mapping[pid];

        Leaf<Key, Data> *tempNode = createConsolidatedLeafPage(startNode); //TODO perhaps more intelligent
        Key Kp = std::get<0>(tempNode->records[tempNode->recordCount / 2]);

        Leaf<Key, Data> *newRightNode = createConsolidatedLeafPage(startNode, Kp);
        newRightNode->prev = pid;
        PID newRightNodePID = newNode(newRightNode);

        DeltaSplit<Key, Data> *splitNode = CreateDeltaSplit(startNode, Kp, newRightNodePID);

        auto c = newRightNode->recordCount; auto b = tempNode->recordCount;
        if (!mapping[pid].compare_exchange_weak(startNode, splitNode)) {
            ++atomicCollisions;
            ++failedSplit;
            free(splitNode);
            free(newRightNode);
            mapping[newRightNodePID].store(nullptr);
            //splitLeafPage(pid);//TODO without recursion // TODO does removing this prevent double splitting?
            return;
        } else {
            ++successfulSplit;
        }
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::consolidateLeafPage(std::stack<PID> &&stack) {
        PID pid = stack.top();
        stack.pop();
        Node<Key, Data> *startNode = mapping[pid];
        Leaf<Key, Data> *newNode = createConsolidatedLeafPage(startNode);
        auto c = newNode->recordCount;
        Node<Key, Data> *previousNode = startNode;

        if (!mapping[pid].compare_exchange_weak(startNode, newNode)) {
            ++atomicCollisions;
            ++failedConsolidate;
            //consolidateLeafPage(pid); // TODO correct
        } else {
            ++successfulConsolidate;
            markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    Leaf<Key, Data> *Tree<Key, Data>::createConsolidatedLeafPage(Node<Key, Data> *startNode, Key keysGreaterThan) {
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
                        auto &curKey = std::get<0>(node1->records[i]);
                        if (curKey > keysGreaterThan && (!pageSplit || curKey <= stopAtKey) && consideredKeys.find(curKey) == consideredKeys.end()) {
                            records.push_back(node1->records[i]);
                            consideredKeys[curKey] = true;
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
                    auto &curKey = std::get<0>(node1->record);
                    if (curKey > keysGreaterThan && (!pageSplit || curKey <= stopAtKey) && consideredKeys.find(curKey) == consideredKeys.end()) {
                        records.push_back(node1->record);
                        consideredKeys[curKey] = true;
                    }
                    node = node1->origin;
                    continue;
                }
                case PageType::deltaDelete: {
                    auto node1 = static_cast<DeltaDelete<Key, Data> *>(node);
                    if (consideredKeys.find(node1->key) == consideredKeys.end()) {
                        consideredKeys[node1->key] = true;
                    }
                    node = node1->origin;
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
        auto newNode = CreateLeaf<Key, Data>(records.size(), next, prev);
        std::sort(records.begin(), records.end(), [](const std::tuple<Key, const Data *> &t1, const std::tuple<Key, const Data *> &t2) {
            return std::get<0>(t1) < std::get<0>(t2);
        });
        int i = 0;
        for (auto &r : records) {
            newNode->records[i++] = r;
        }
        return newNode;
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
















#endif
