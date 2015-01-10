#ifndef BWTREE_CPP
#define BWTREE_CPP

#include <unordered_map>
#include "bwtree.hpp"
#include <cassert>
#include <algorithm>
#include <unordered_set>
#include <sys/time.h>

namespace BwTree {

    template<typename Key, typename Data>
    Data *Tree<Key, Data>::search(Key key) {
        unsigned e = epoque.enterEpoque();
        FindDataPageResult<Key, Data> res = findDataPage(key);
        Data *returnValue;
        if (res.dataNode == nullptr) {
            returnValue = nullptr;
        } else {
            returnValue = const_cast<Data *>(res.data);
        }
        if (res.needSplitPage.size() > 0) {
            splitPage(std::move(res.needSplitPage));
        } else if (res.needConsolidatePage.size() > 0) {
            consolidatePage(std::move(res.needConsolidatePage));
        }
        epoque.leaveEpoque(e);
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
            if (debugTMPCheck++ > 50000) {
                assert(false);
                //std::cout << debugTMPCheck << std::endl;
            }
            std::size_t pageDepth = 0;
            parentNodes.push(nextPID);
            Node<Key, Data> *startNode = PIDToNodePtr(nextPID);
            Node<Key, Data> *nextNode = startNode;
            std::size_t deltaLength = 0;
            while (nextNode != nullptr) {
                ++pageDepth;
                if (pageDepth == settings.ConsolidateLeafPage && needConsolidatePage.size() == 0) {//TODO save for later
                    needConsolidatePage = parentNodes;
                }
                switch (nextNode->type) {
                    case PageType::deltaIndex: {
                        auto node1 = static_cast<DeltaIndex<Key, Data> *>(nextNode);
                        if (key > node1->keyLeft && key <= node1->keyRight) {
                            nextPID = node1->child;
                            nextNode = nullptr;
                            break;
                        } else {
                            deltaLength++;
                            nextNode = node1->origin;
                            continue;
                        }
                    };
                    case PageType::inner: {
                        auto node1 = static_cast<InnerNode<Key, Data> *>(nextNode);
                        if (node1->nodeCount + deltaLength > settings.SplitInnerPage && needSplitPage.size() == 0) {
                            needSplitPage = parentNodes;
                        }
                        auto res = binarySearch<decltype(node1->nodes)>(node1->nodes, node1->nodeCount, key);
                        if (res == node1->nodeCount && node1->next != NotExistantPID) {
                            parentNodes.pop();// to keep correct parent history upward
                            nextPID = node1->next;
                        } else {
                            nextPID = std::get<1>(node1->nodes[res]);
                        }
                        nextNode = nullptr;
                        break;
                    };
                    case PageType::leaf: {
                        auto node1 = static_cast<Leaf<Key, Data> *>(nextNode);
                        if (node1->recordCount + deltaLength > settings.SplitLeafPage && needSplitPage.size() == 0) {
                            needSplitPage = parentNodes;
                        }
                        auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, key);
                        if (node1->recordCount > res && std::get<0>(node1->records[res]) == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key, std::get<1>(node1->records[res]), std::move(needConsolidatePage), std::move(needSplitPage), std::move(parentNodes));
                        } else {
                            if (res == node1->recordCount && node1->next != NotExistantPID) {
                                parentNodes.pop();// to keep correct parent history upward
                                nextPID = node1->next;
                                nextNode = nullptr;
                                continue;
                            }
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nullptr, std::move(needConsolidatePage), std::move(needSplitPage), std::move(parentNodes));
                        }
                    };
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                        if (std::get<0>(node1->record) == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key, std::get<1>(node1->record), std::move(needConsolidatePage), std::move(needSplitPage), std::move(parentNodes));
                        }
                        deltaLength++;
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    case PageType::deltaDelete: {
                        auto node1 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                        if (node1->key == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nullptr, std::move(needConsolidatePage), std::move(needSplitPage), std::move(parentNodes));
                        }
                        deltaLength--;
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    case PageType::deltaSplitInner:
                    case PageType::deltaSplit: {
                        auto node1 = static_cast<DeltaSplit<Key, Data> *>(nextNode);
                        if (key > node1->key) {
                            nextPID = node1->sidelink;
                            nextNode = startNode = nullptr;
                            parentNodes.pop();
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
        unsigned e = epoque.enterEpoque();
        FindDataPageResult<Key, Data> res = findDataPage(key);
        switch (res.startNode->type) {
            case PageType::deltaIndex:
            case PageType::inner:
            case PageType::deltaSplitInner:
                assert(false); // only leafs  should come to here
            case PageType::deltaInsert:
            case PageType::deltaDelete:
            case PageType::deltaSplit:
            case PageType::leaf: {
                DeltaInsert<Key, Data> *newNode = CreateDeltaInsert<Key, Data>(res.startNode, std::make_tuple(key, record));
                if (!mapping[res.pid].compare_exchange_weak(res.startNode, newNode)) {
                    ++atomicCollisions;
                    free(newNode);
                    epoque.leaveEpoque(e);
                    insert(key, record);
                    return;
                } else {
                    if (res.needSplitPage.size() > 0) {
                        splitPage(std::move(res.needSplitPage));
                    } else if (res.needConsolidatePage.size() > 0) {
                        consolidatePage(std::move(res.needConsolidatePage));
                    }
                    epoque.leaveEpoque(e);
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
        unsigned e = epoque.enterEpoque();
        FindDataPageResult<Key, Data> res = findDataPage(key);
        if (res.dataNode == nullptr) {
            epoque.leaveEpoque(e);
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
                epoque.leaveEpoque(e);
                return;
            }
        }
        assert(false);
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::splitPage(PID pid, bool leaf, Node<Key, Data> *node, std::stack<PID> &&stack) {
        struct timeval starttime;
        gettimeofday(&starttime, NULL);

        Node<Key, Data> *startNode = mapping[pid];

        Key Kp, Kq;
        LinkedNode<Key, Data> *newRightNode;
        if (!leaf) {
            std::vector<std::tuple<Key, PID>> nodes;
            PID prev, next;
            bool hadInfinityElement;
            std::tie(prev, next, hadInfinityElement) = std::move(getConsolidatedInnerData(node, nodes));
            if (nodes.size() < settings.SplitInnerPage) {
                return;
            }
            auto middle = nodes.begin();
            std::advance(middle, (std::distance(nodes.begin(), nodes.end()) / 2) - 1);
            std::nth_element(nodes.begin(), middle, nodes.end(), [](const std::tuple<Key, PID> &t1, const std::tuple<Key, PID> &t2) {
                return std::get<0>(t1) < std::get<0>(t2);
            });

            Kp = std::get<0>(*middle);

            auto newRightInner = Helper<Key, Data>::CreateInnerNodeFromUnsorted(middle + 1, nodes.end(), next, pid, hadInfinityElement);
            assert(newRightInner->nodeCount > 0);
            Kq = std::get<0>(newRightInner->nodes[newRightInner->nodeCount - 1]);
            newRightNode = newRightInner;
        } else {
            Leaf<Key, Data> *tempNode = createConsolidatedLeafPage(startNode); //TODO perhaps more intelligent
            if (tempNode->recordCount < settings.SplitLeafPage) {
                return;
            }
            Kp = std::get<0>(tempNode->records[tempNode->recordCount / 2]);
            free(tempNode);
            Leaf<Key, Data> *newRightLeaf = createConsolidatedLeafPage(startNode, Kp);
            assert(newRightLeaf->recordCount > 0);
            Kq = std::get<0>(newRightLeaf->records[newRightLeaf->recordCount - 1]);
            newRightNode = newRightLeaf;
        }


        newRightNode->prev = pid;
        PID newRightNodePID = newNode(newRightNode);
        DeltaSplit<Key, Data> *splitNode;
        if (!leaf) {
            splitNode = CreateDeltaSplitInner(startNode, Kp, newRightNodePID);
        } else {
            splitNode = CreateDeltaSplit(startNode, Kp, newRightNodePID);
        }

        if (!mapping[pid].compare_exchange_weak(startNode, splitNode)) {
            ++atomicCollisions;
            if (!leaf) ++failedInnerSplit; else ++failedLeafSplit;
            free(splitNode);
            free(newRightNode);
            mapping[newRightNodePID].store(nullptr);
            return;
        } else {
            struct timeval end;
            gettimeofday(&end, NULL);
            double delta = (end.tv_sec - starttime.tv_sec) * 1.0 + (end.tv_usec - starttime.tv_usec) * 0.000001;
            if (!leaf) {
                timeForInnerSplit.store(timeForInnerSplit + delta);

                ++successfulInnerSplit;
            } else {
                timeForLeafSplit.store(timeForLeafSplit + delta);
                ++successfulLeafSplit;
            }
        }
        while (true) {
            if (stack.empty()) {
                InnerNode<Key, Data> *newRoot = CreateInnerNode<Key, Data>(2, NotExistantPID, NotExistantPID);
                std::get<0>(newRoot->nodes[0]) = Kp;
                std::get<1>(newRoot->nodes[0]) = pid;
                std::get<0>(newRoot->nodes[1]) = Kq;
                std::get<1>(newRoot->nodes[1]) = newRightNodePID;
                PID newRootPid = newNode(newRoot);
                if (!root.compare_exchange_weak(pid, newRootPid)) {
                    free(newRoot);
                    ++atomicCollisions;
                    stack.push(root.load());//TODO does this work?
                } else {
                    return;
                }
            } else {
                PID parentPid = stack.top();
                Node<Key, Data> *parentNode = PIDToNodePtr(parentPid);
                assert(parentNode->type == PageType::inner || parentNode->type == PageType::deltaIndex || parentNode->type == PageType::deltaSplitInner);
                while (true) {
                    DeltaIndex<Key, Data> *indexNode = CreateDeltaIndex(parentNode, Kp, Kq, newRightNodePID, pid);
                    if (!mapping[parentPid].compare_exchange_weak(parentNode, indexNode)) {
                        free(indexNode);
                        ++atomicCollisions;
                    } else {
                        return;
                    }
                }
            }
        }
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::consolidateLeafPage(PID pid, Node<Key, Data> *node) {
        struct timeval starttime;
        gettimeofday(&starttime, NULL);

        Node<Key, Data> *startNode = mapping[pid];
        Leaf<Key, Data> *newNode = createConsolidatedLeafPage(startNode);
        Node<Key, Data> *previousNode = startNode;

        if (!mapping[pid].compare_exchange_weak(startNode, newNode)) {
            free(newNode);
            ++atomicCollisions;
            ++failedLeafConsolidate;
        } else {
            struct timeval end;
            gettimeofday(&end, NULL);
            timeForLeafConsolidation.store(timeForLeafConsolidation + (end.tv_sec - starttime.tv_sec) * 1.0 + (end.tv_usec - starttime.tv_usec) * 0.000001);

            ++successfulLeafConsolidate;
            epoque.markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    Leaf<Key, Data> *Tree<Key, Data>::createConsolidatedLeafPage(Node<Key, Data> *node, Key keysGreaterThan) {
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
    void Tree<Key, Data>::consolidateInnerPage(PID pid, Node<Key, Data> *node) {
        struct timeval starttime;
        gettimeofday(&starttime, NULL);

        Node<Key, Data> *startNode = mapping[pid];
        InnerNode<Key, Data> *newNode = createConsolidatedInnerPage(startNode);
        Node<Key, Data> *previousNode = startNode;

        if (!mapping[pid].compare_exchange_weak(startNode, newNode)) {
            free(newNode);
            ++atomicCollisions;
            ++failedInnerConsolidate;
        } else {
            struct timeval end;
            gettimeofday(&end, NULL);
            timeForInnerConsolidation.store(timeForInnerConsolidation + (end.tv_sec - starttime.tv_sec) * 1.0 + (end.tv_usec - starttime.tv_usec) * 0.000001);

            ++successfulInnerConsolidate;
            epoque.markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    std::tuple<PID, PID, bool> Tree<Key, Data>::getConsolidatedInnerData(Node<Key, Data> *node, std::vector<std::tuple<Key, PID>> &nodes) {
        std::unordered_set<PID> consideredPIDs;
        Key stopAtKey;
        bool pageSplit = false;
        PID prev, next;
        Node<Key, Data> *startNode = node;
        bool hadInfinityElement = false;
        while (node != nullptr) {
            switch (node->type) {
                case PageType::inner: {
                    auto node1 = static_cast<InnerNode<Key, Data> *>(node);
                    for (int i = 0; i < node1->nodeCount; ++i) {
                        auto &curKey = std::get<0>(node1->nodes[i]);
                        if ((!pageSplit || curKey <= stopAtKey) && consideredPIDs.find(std::get<1>(node1->nodes[i])) == consideredPIDs.end()) {
                            nodes.push_back(node1->nodes[i]);
                        }
                    }
                    if (!pageSplit && std::get<0>(node1->nodes[node1->nodeCount - 1]) == NotExistantPID) {
                        hadInfinityElement = true;
                    }
                    prev = node1->prev;
                    if (!pageSplit) {
                        next = node1->next;
                    }
                    // found last element in the chain
                    break;
                }
                case PageType::deltaIndex: {
                    auto node1 = static_cast<DeltaIndex<Key, Data> *>(node);
                    if ((!pageSplit || node1->keyRight <= stopAtKey)) {
                        if (consideredPIDs.find(node1->oldChild) == consideredPIDs.end()) {
                            nodes.push_back(std::make_tuple(node1->keyLeft, node1->oldChild));
                            consideredPIDs.emplace(node1->oldChild);
                        }
                        if (consideredPIDs.find(node1->child) == consideredPIDs.end()) {
                            nodes.push_back(std::make_tuple(node1->keyRight, node1->child));
                            consideredPIDs.emplace(node1->child);
                        }
                    }
                    node = node1->origin;
                    continue;
                }
                case PageType::deltaSplitInner: {
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
        return std::make_tuple(prev, next, hadInfinityElement);
    }

    template<typename Key, typename Data>
    InnerNode<Key, Data> *Tree<Key, Data>::createConsolidatedInnerPage(Node<Key, Data> *node) {
        std::vector<std::tuple<Key, PID>> nodes;
        PID prev, next;
        bool hadInfinityElement;
        std::tie(prev, next, hadInfinityElement) = std::move(getConsolidatedInnerData(node, nodes));
        return Helper<Key, Data>::CreateInnerNodeFromUnsorted(nodes.begin(), nodes.end(), next, prev, hadInfinityElement);
    }

    template<typename Key, typename Data>
    Tree<Key, Data>::~Tree() {
        for (int i = 0; i < mappingNext; ++i) {
            Node<Key, Data> *node = mapping[i];
            freeNodeRecursively<Key, Data>(node);
        }
    }
}

#endif
