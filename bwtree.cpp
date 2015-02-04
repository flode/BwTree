#ifndef BWTREE_CPP
#define BWTREE_CPP

#include <unordered_map>
#include "bwtree.hpp"
#include <cassert>
#include <algorithm>
#include <unordered_set>

namespace BwTree {

    template<typename Key, typename Data>
    Data *Tree<Key, Data>::search(Key key) {
        EnterEpoque<Key, Data> epoqueGuard(epoque);
        FindDataPageResult<Key, Data> res = findDataPage(key);
        Data *returnValue;
        if (res.dataNode == nullptr) {
            returnValue = nullptr;
        } else {
            returnValue = const_cast<Data *>(res.data);
        }
        if (res.needSplitPage != NotExistantPID) {
            splitPage(res.needSplitPage, res.needSplitPageParent);
        } else if (res.needConsolidatePage != NotExistantPID) {
            consolidatePage(res.needConsolidatePage);
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
        PID needConsolidatePage = NotExistantPID;
        PID needSplitPage = NotExistantPID;
        PID needSplitPageParent = NotExistantPID;
        PID parent = NotExistantPID;
        bool doNotSplit = false;
        int level = 0;
        while (nextPID != NotExistantPID) {
            if (debugTMPCheck++ > 50000) {
                assert(false);
                //std::cout << debugTMPCheck << std::endl;
            }
            std::size_t pageDepth = 0;
            Node<Key, Data> *nextNode = PIDToNodePtr(nextPID);
            if (isLeaf(nextNode)) {
                break;
            }
            long deltaNodeCount = 0;
            long removedBySplit = 0;
            while (nextNode != nullptr) {
                ++pageDepth;
                assert(pageDepth < 10000);
                if (needConsolidatePage == NotExistantPID && (
                        (pageDepth == settings.getConsolidateLimitInner(level) && this->rand(this->d) - level < 40)
                )) {//TODO save for later
                    needConsolidatePage = nextPID;
                }
                switch (nextNode->type) {
                    case PageType::deltaIndex: {
                        auto node1 = static_cast<DeltaIndex<Key, Data> *>(nextNode);
                        if (key > node1->keyLeft && key <= node1->keyRight) {
                            level++;
                            parent = nextPID;
                            doNotSplit = false;
                            nextPID = node1->child;
                            nextNode = nullptr;
                            continue;
                        } else {
                            deltaNodeCount++;
                            nextNode = node1->origin;
                            continue;
                        }
                    };
                    case PageType::inner: {
                        auto node1 = static_cast<InnerNode<Key, Data> *>(nextNode);
                        if (!doNotSplit && (node1->nodeCount + deltaNodeCount - removedBySplit) > settings.getSplitLimitInner(level) && needSplitPage == NotExistantPID && this->rand(this->d) - level * 5 < 30) {
                            if (DEBUG) std::cout << "inner count" << node1->nodeCount << " " << deltaNodeCount << " " << removedBySplit;
                            needSplitPage = nextPID;
                            needSplitPageParent = parent;
                        }
                        auto res = binarySearch<decltype(node1->nodes)>(node1->nodes, node1->nodeCount, key);
                        if (res == node1->nodeCount && node1->next != NotExistantPID) {
                            doNotSplit = true;
                            nextPID = node1->next;
                        } else {
                            level++;
                            parent = nextPID;
                            doNotSplit = false;
                            nextPID = std::get<1>(node1->nodes[res]);
                        }
                        nextNode = nullptr;
                        continue;
                    };
                    case PageType::deltaSplitInner: {
                        auto node1 = static_cast<DeltaSplit<Key, Data> *>(nextNode);
                        if (key > node1->key) {
                            nextPID = node1->sidelink;
                            nextNode = nullptr;
                            doNotSplit = true;
                            continue;
                        }
                        removedBySplit += node1->removedElements;
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

        // Handle leaf
        while (nextPID != NotExistantPID) {
            if (debugTMPCheck++ > 50000) {
                assert(false);
                //std::cout << debugTMPCheck << std::endl;
            }
            std::size_t pageDepth = 0;
            Node<Key, Data> *startNode = PIDToNodePtr(nextPID);
            Node<Key, Data> *nextNode = startNode;
            long deltaNodeCount = 0;
            long removedBySplit = 0;
            while (nextNode != nullptr) {
                ++pageDepth;
                assert(pageDepth < 10000);
                if (needConsolidatePage == NotExistantPID && pageDepth == settings.getConsolidateLimitLeaf()) {
                    //TODO save for later
                    needConsolidatePage = nextPID;
                }
                switch (nextNode->type) {
                    case PageType::leaf: {
                        auto node1 = static_cast<Leaf<Key, Data> *>(nextNode);
                        if (!doNotSplit && node1->recordCount + deltaNodeCount - removedBySplit > settings.getSplitLimitLeaf() && needSplitPage == NotExistantPID) {
                            if (DEBUG) std::cout << "leaf count" << node1->recordCount << " " << deltaNodeCount << " " << removedBySplit;
                            needSplitPage = nextPID;
                            needSplitPageParent = parent;
                        }
                        auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, key);
                        if (node1->recordCount > res && std::get<0>(node1->records[res]) == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key, std::get<1>(node1->records[res]), needConsolidatePage, needSplitPage, needSplitPageParent);
                        } else {
                            if (res == node1->recordCount && node1->next != NotExistantPID) {
                                doNotSplit = true;
                                nextPID = node1->next;
                                nextNode = nullptr;
                                continue;
                            }
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nullptr, needConsolidatePage, needSplitPage, needSplitPageParent);
                        }
                    };
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                        if (std::get<0>(node1->record) == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key, std::get<1>(node1->record), needConsolidatePage, needSplitPage, needSplitPageParent);
                        }
                        deltaNodeCount++;
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    case PageType::deltaDelete: {
                        auto node1 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                        if (node1->key == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nullptr, needConsolidatePage, needSplitPage, needSplitPageParent);
                        }
                        deltaNodeCount--;
                        nextNode = node1->origin;
                        assert(nextNode != nullptr);
                        continue;
                    };
                    case PageType::deltaSplit: {
                        auto node1 = static_cast<DeltaSplit<Key, Data> *>(nextNode);
                        if (key > node1->key) {
                            nextPID = node1->sidelink;
                            nextNode = startNode = nullptr;
                            doNotSplit = true;
                            continue;
                        }
                        removedBySplit += node1->removedElements;
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
        return FindDataPageResult<Key, Data>(NotExistantPID, nullptr, nullptr, needConsolidatePage, needSplitPage, needSplitPageParent);
    }

    template<typename Key, typename Data>
    std::tuple<PID, Node<Key, Data> *> Tree<Key, Data>::findInnerNodeOnLevel(PID pid, const Key key) {
        PID nextPID = pid;
        std::size_t debugTMPCheck = 0;
        while (nextPID != NotExistantPID) {
            if (++debugTMPCheck > 0)
                assert(++debugTMPCheck < 100);
            Node<Key, Data> *startNode = PIDToNodePtr(nextPID);
            Node<Key, Data> *nextNode = startNode;
            while (nextNode != nullptr) {
                switch (nextNode->type) {
                    case PageType::deltaIndex: {
                        auto node1 = static_cast<DeltaIndex<Key, Data> *>(nextNode);
                        if (key > node1->keyLeft && key <= node1->keyRight) {
                            return std::make_tuple(nextPID, startNode);
                        } else {
                            nextNode = node1->origin;
                            continue;
                        }
                    };
                    case PageType::inner: {
                        auto node1 = static_cast<InnerNode<Key, Data> *>(nextNode);
                        auto res = binarySearch<decltype(node1->nodes)>(node1->nodes, node1->nodeCount, key);
                        if (res == node1->nodeCount && node1->next != NotExistantPID) {
                            nextPID = node1->next;
                        } else {
                            return std::make_tuple(nextPID, startNode);
                        }
                        nextNode = nullptr;
                        continue;
                    };
                    case PageType::deltaSplitInner: {
                        auto node1 = static_cast<DeltaSplit<Key, Data> *>(nextNode);
                        if (key > node1->key) {
                            nextPID = node1->sidelink;
                            nextNode = nullptr;
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
        assert(false);
        return std::make_tuple(NotExistantPID, nullptr);
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::insert(Key key, const Data *const record) {
        EnterEpoque<Key, Data> epoqueGuard(epoque);
        restartInsert:
        FindDataPageResult<Key, Data> res = findDataPage(key);
        assert(isLeaf(res.startNode));
        if (res.needConsolidatePage == res.pid) {
            consolidateLeafPage(res.pid, res.startNode);
            goto restartInsert;
        }
        DeltaInsert<Key, Data> *newNode = DeltaInsert<Key, Data>::create(res.startNode, std::make_tuple(key, record), (res.dataNode != nullptr));
        if (!mapping[res.pid].compare_exchange_weak(res.startNode, newNode)) {
            ++atomicCollisions;
            freeNodeSingle<Key, Data>(newNode);
            goto restartInsert;
        } else {
            if (res.needSplitPage != NotExistantPID) {
                splitPage(res.needSplitPage, res.needSplitPageParent);
            } else if (res.needConsolidatePage != NotExistantPID) {
                consolidatePage(res.needConsolidatePage);
            }
            return;
        }
    }


    template<typename Key, typename Data>
    void Tree<Key, Data>::deleteKey(Key key) {
        EnterEpoque<Key, Data> epoqueGuard(epoque);
        restartDelete:
        FindDataPageResult<Key, Data> res = findDataPage(key);
        if (res.dataNode == nullptr) {
            return;
        }
        assert(isLeaf(res.startNode));
        DeltaDelete<Key, Data> *newDeleteNode = DeltaDelete<Key, Data>::create(res.startNode, key);
        if (!mapping[res.pid].compare_exchange_weak(res.startNode, newDeleteNode)) {
            ++atomicCollisions;
            freeNodeSingle<Key, Data>(newDeleteNode);
            goto restartDelete;
        }
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::splitPage(const PID needSplitPage, PID needSplitPageParent) {
        assert(needSplitPage != needSplitPageParent);
        if (DEBUG) std::cout << "split page" << std::endl;
        Node<Key, Data> *startNode = PIDToNodePtr(needSplitPage);
        bool leaf = isLeaf(startNode);
        auto starttime = std::chrono::system_clock::now();

        Key Kp, Kq;
        LinkedNode<Key, Data> *newRightNode;
        std::size_t removedElements;
        if (!leaf) {
            static thread_local std::vector<std::tuple<Key, PID>> nodesStatic;
            std::vector<std::tuple<Key, PID>> &nodes = nodesStatic;
            nodes.clear();
            PID prev, next;
            bool hadInfinityElement;
            std::tie(prev, next, hadInfinityElement) = getConsolidatedInnerData(startNode, needSplitPage, nodes);
            if (nodes.size() < settings.getSplitLimitInner(0)) {//TODO exact level needed?
                return;
            }
            if (DEBUG) std::cout << "inner size: " << nodes.size() << std::endl;
            auto middle = nodes.begin();
            std::advance(middle, (std::distance(nodes.begin(), nodes.end()) / 2) - 1);
            std::nth_element(nodes.begin(), middle, nodes.end(), [](const std::tuple<Key, PID> &t1, const std::tuple<Key, PID> &t2) {
                return std::get<0>(t1) < std::get<0>(t2);
            });

            Kp = std::get<0>(*middle);

            auto newRightInner = Helper<Key, Data>::CreateInnerNodeFromUnsorted(middle + 1, nodes.end(), needSplitPage, next, hadInfinityElement);
            assert(newRightInner->nodeCount > 0);
            Kq = std::get<0>(newRightInner->nodes[newRightInner->nodeCount - 1]);
            removedElements = newRightInner->nodeCount;
            newRightNode = newRightInner;
        } else {
            static thread_local std::vector<std::tuple<Key, const Data *>> recordsStatic;
            std::vector<std::tuple<Key, const Data *>> &records = recordsStatic;
            records.clear();
            PID prev, next;
            std::tie(prev, next) = getConsolidatedLeafData(startNode, records);
            if (DEBUG) std::cout << "leaf size: " << records.size() << std::endl;
            if (records.size() < settings.getSplitLimitLeaf()) {
                return;
            }
            auto middle = records.begin();
            std::advance(middle, (std::distance(records.begin(), records.end()) / 2) - 1);
            std::nth_element(records.begin(), middle, records.end(), [](const std::tuple<Key, const Data *> &t1, const std::tuple<Key, const Data *> &t2) {
                return std::get<0>(t1) < std::get<0>(t2);
            });

            Kp = std::get<0>(*middle);

            auto newRightLeaf = Helper<Key, Data>::CreateLeafNodeFromUnsorted(middle + 1, records.end(), needSplitPage, next);
            assert(newRightLeaf->recordCount > 0);
            Kq = std::get<0>(newRightLeaf->records[newRightLeaf->recordCount - 1]);
            removedElements = newRightLeaf->recordCount;
            newRightNode = newRightLeaf;
        }


        newRightNode->prev = needSplitPage;
        const PID newRightNodePID = newNode(newRightNode);
        DeltaSplit<Key, Data> *splitNode;

        splitNode = DeltaSplit<Key, Data>::create(startNode, Kp, newRightNodePID, removedElements, leaf);

        if (!mapping[needSplitPage].compare_exchange_strong(startNode, splitNode)) {
            ++atomicCollisions;
            if (!leaf) ++failedInnerSplit; else ++failedLeafSplit;
            freeNodeSingle<Key, Data>(splitNode);
            freeNodeSingle<Key, Data>(newRightNode);
            mapping[newRightNodePID].store(nullptr);
            return;
        } else {
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - starttime);
            if (!leaf) {
                timeForInnerSplit.store(timeForInnerSplit + duration.count());

                ++successfulInnerSplit;
            } else {
                timeForLeafSplit.store(timeForLeafSplit + duration.count());
                ++successfulLeafSplit;
            }
        }

        if (needSplitPageParent == NotExistantPID) {
            InnerNode<Key, Data> *newRoot = InnerNode<Key, Data>::create(2, NotExistantPID, NotExistantPID);
            std::get<0>(newRoot->nodes[0]) = Kp;
            std::get<1>(newRoot->nodes[0]) = needSplitPage;
            std::get<0>(newRoot->nodes[1]) = std::numeric_limits<Key>::max();
            std::get<1>(newRoot->nodes[1]) = newRightNodePID;
            PID newRootPid = newNode(newRoot);
            PID curRoot = needSplitPage;
            if (root.compare_exchange_strong(curRoot, newRootPid)) {
                return;
            }
            freeNodeSingle<Key, Data>(newRoot);
            ++atomicCollisions;
            needSplitPageParent = root.load();
        }
        assert(root.load() != needSplitPage);
        std::size_t TMPsplitCollisions = 0;
        while (true) {
            Node<Key, Data> *parentNode = PIDToNodePtr(needSplitPageParent);
            assert(parentNode->type == PageType::inner || parentNode->type == PageType::deltaIndex || parentNode->type == PageType::deltaSplitInner);
            DeltaIndex<Key, Data> *indexNode = DeltaIndex<Key, Data>::create(parentNode, Kp, Kq, newRightNodePID, needSplitPage);
            if (!mapping[needSplitPageParent].compare_exchange_strong(parentNode, indexNode)) {
                freeNodeSingle<Key, Data>(indexNode);
                ++atomicCollisions;
                // check if parent has been split
                std::tie(needSplitPageParent, parentNode) = findInnerNodeOnLevel(needSplitPageParent, Kq);
                if (++TMPsplitCollisions > 0)
                    assert(TMPsplitCollisions < 100);
            } else {
                return;
            }
        }

    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::consolidateLeafPage(const PID pid, Node<Key, Data> *startNode) {
        if (DEBUG) std::cout << "consolidate leaf page" << std::endl;
        auto starttime = std::chrono::system_clock::now();

        static thread_local std::vector<std::tuple<Key, const Data *>> recordsStatic;
        std::vector<std::tuple<Key, const Data *>> &records = recordsStatic;
        records.clear();
        PID prev, next;
        std::tie(prev, next) = getConsolidatedLeafData(startNode, records);
        Leaf<Key, Data> *newNode = Helper<Key, Data>::CreateLeafNodeFromUnsorted(records.begin(), records.end(), prev, next);

        Node<Key, Data> *previousNode = startNode;

        if (!mapping[pid].compare_exchange_strong(startNode, newNode)) {
            freeNodeSingle<Key, Data>(newNode);
            ++atomicCollisions;
            ++failedLeafConsolidate;
        } else {
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - starttime);
            timeForLeafConsolidation.store(timeForLeafConsolidation + duration.count());

            ++successfulLeafConsolidate;
            epoque.markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    std::tuple<PID, PID> Tree<Key, Data>::getConsolidatedLeafData(Node<Key, Data> *node, std::vector<std::tuple<Key, const Data *>> &records) {
        std::array<std::tuple<Key, const Data *>, 100> deltaInsertRecords;
        std::array<Key, 100> consideredKeys;
        std::size_t deltaInsertRecordsNext = 0;
        std::size_t consideredKeysNext = 0;
        Key stopAtKey = std::numeric_limits<Key>::max();
        bool pageSplit = false;
        PID prev, next;
        while (node->type != PageType::leaf) {
            switch (node->type) {
                case PageType::deltaInsert: {
                    auto node1 = static_cast<DeltaInsert<Key, Data> *>(node);
                    auto &curKey = std::get<0>(node1->record);
                    if (curKey <= stopAtKey
                            && std::find(consideredKeys.begin(), consideredKeys.begin() + consideredKeysNext, curKey) == consideredKeys.begin() + consideredKeysNext) {
                        deltaInsertRecords[deltaInsertRecordsNext++] = node1->record;
                        assert(deltaInsertRecordsNext != deltaInsertRecords.size());
                        if (node1->keyExistedBefore) {
                            consideredKeys[consideredKeysNext++] = curKey;
                            assert(consideredKeysNext != consideredKeys.size());
                        }
                    }
                    node = node1->origin;
                    continue;
                }
                case PageType::deltaDelete: {
                    auto node1 = static_cast<DeltaDelete<Key, Data> *>(node);
                    auto &curKey = node1->key;
                    if (std::find(consideredKeys.begin(), consideredKeys.begin() + consideredKeysNext, curKey) == consideredKeys.begin() + consideredKeysNext) {
                        consideredKeys[consideredKeysNext++] = curKey;
                        assert(consideredKeysNext != consideredKeys.size());
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
        }
        //case PageType::leaf:
        const auto node1 = static_cast<Leaf<Key, Data> *>(node);
        std::sort(deltaInsertRecords.begin(), deltaInsertRecords.begin() + deltaInsertRecordsNext, [](const std::tuple<Key, const Data *> &t1, const std::tuple<Key, const Data *> &t2) {
            return std::get<0>(t1) < std::get<0>(t2);
        });
        std::sort(consideredKeys.begin(), consideredKeys.begin() + consideredKeysNext);
        std::tuple<Key, const Data *> *recordsdata[] = {const_cast<std::tuple<Key, const Data *> *>(deltaInsertRecords.data()), node1->records};
        std::size_t nextconsidered = 0;
        std::size_t nextrecords[] = {0, 0};
        std::size_t &nextrecord = nextrecords[1];
        std::size_t &nextdelta = nextrecords[0];
        while (nextrecord < node1->recordCount && nextdelta < deltaInsertRecordsNext) {
            nextconsidered += (nextconsidered < consideredKeysNext && std::get<0>(node1->records[nextrecord]) > consideredKeys[nextconsidered]);
            if (nextconsidered < consideredKeysNext && std::get<0>(node1->records[nextrecord]) > consideredKeys[nextconsidered]) {
                continue;
            }
            nextrecord += (nextconsidered < consideredKeysNext && std::get<0>(node1->records[nextrecord]) == consideredKeys[nextconsidered]);
            if (nextconsidered < consideredKeysNext && std::get<0>(node1->records[nextrecord]) == consideredKeys[nextconsidered]) {
                continue;
            }

            std::int8_t choice = (std::get<0>(node1->records[nextrecord]) < std::get<0>(deltaInsertRecords[nextdelta]));
            std::tuple<Key, const Data *> record = recordsdata[choice][nextrecords[choice]];
            ++nextrecords[choice];
            if (std::get<0>(record) <= stopAtKey) {
                records.push_back(record);
            } else {
                nextrecord = node1->recordCount;
                nextdelta = deltaInsertRecordsNext;
                break;
            }
        }
        while (nextrecord < node1->recordCount) {
            if (std::get<0>(node1->records[nextrecord]) <= stopAtKey) {
                records.push_back(node1->records[nextrecord]);
                ++nextrecord;
            } else {
                break;
            }
        }
        while (nextdelta < deltaInsertRecordsNext) {
            if (std::get<0>(deltaInsertRecords[nextdelta]) <= stopAtKey) {
                records.push_back(deltaInsertRecords[nextdelta]);
                ++nextdelta;
            } else {
                break;
            }
        }
        prev = node1->prev;
        if (!pageSplit) {
            next = node1->next;
        }
        return std::make_tuple(prev, next);
    }

    template<typename Key, typename Data>
    void Tree<Key, Data>::consolidateInnerPage(const PID pid, Node<Key, Data> *startNode) {
        if (DEBUG) std::cout << "consolidate inner page" << std::endl;
        auto starttime = std::chrono::system_clock::now();

        static thread_local std::vector<std::tuple<Key, PID>> nodesStatic;
        std::vector<std::tuple<Key, PID>> &nodes = nodesStatic;
        nodes.clear();
        PID prev, next;
        bool hadInfinityElement;
        std::tie(prev, next, hadInfinityElement) = getConsolidatedInnerData(startNode, pid, nodes);
        InnerNode<Key, Data> *newNode = Helper<Key, Data>::CreateInnerNodeFromUnsorted(nodes.begin(), nodes.end(), prev, next, hadInfinityElement);

        Node<Key, Data> *const previousNode = startNode;

        if (!mapping[pid].compare_exchange_strong(startNode, newNode)) {
            freeNodeSingle<Key, Data>(newNode);
            ++atomicCollisions;
            ++failedInnerConsolidate;
        } else {
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - starttime);
            timeForInnerConsolidation.store(timeForInnerConsolidation + duration.count());

            ++successfulInnerConsolidate;
            epoque.markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    std::tuple<PID, PID, bool> Tree<Key, Data>::getConsolidatedInnerData(Node<Key, Data> *node, PID pid, std::vector<std::tuple<Key, PID>> &nodes) {
        std::array<PID, 40> consideredPIDs;
        consideredPIDs.fill(NotExistantPID);
        std::size_t consideredPIDsNextIndex = 0;

        Key stopAtKey = std::numeric_limits<Key>::max();
        bool pageSplit = false;
        PID prev, next;
        bool hadInfinityElement = false;
        while (node != nullptr) {
            switch (node->type) {
                case PageType::inner: {
                    auto node1 = static_cast<InnerNode<Key, Data> *>(node);
                    for (std::size_t i = 0; i < node1->nodeCount; ++i) {
                        auto &curKey = std::get<0>(node1->nodes[i]);
                        if (curKey <= stopAtKey && std::find(consideredPIDs.begin(), consideredPIDs.begin() + consideredPIDsNextIndex, std::get<1>(node1->nodes[i])) == consideredPIDs.begin() + consideredPIDsNextIndex) {
                            if (std::get<1>(node1->nodes[i]) == pid) {
                                assert(false);
                            }
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
                    if (node1->keyRight <= stopAtKey) {
                        if (std::find(consideredPIDs.begin(), consideredPIDs.begin() + consideredPIDsNextIndex, node1->oldChild) == consideredPIDs.begin() + consideredPIDsNextIndex) {
                            if (node1->oldChild == pid) {
                                assert(false);
                            }
                            nodes.push_back(std::make_tuple(node1->keyLeft, node1->oldChild));
                            consideredPIDs[consideredPIDsNextIndex++] = node1->oldChild;
                            assert(consideredPIDsNextIndex != consideredPIDs.size());
                        }
                        if (std::find(consideredPIDs.begin(), consideredPIDs.begin() + consideredPIDsNextIndex, node1->child) == consideredPIDs.begin() + consideredPIDsNextIndex) {
                            if (node1->child == pid) {
                                assert(false);
                            }
                            nodes.push_back(std::make_tuple(node1->keyRight, node1->child));
                            consideredPIDs[consideredPIDsNextIndex++] = node1->child;
                            assert(consideredPIDsNextIndex != consideredPIDs.size());
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
    Tree<Key, Data>::~Tree() {
        for (unsigned long i = 0; i < mappingNext; ++i) {
            Node<Key, Data> *node = mapping[i];
            freeNodeRecursively<Key, Data>(node);
        }
    }
}

#endif
