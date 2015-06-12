#ifndef BWTREE_CPP
#define BWTREE_CPP

#include <unordered_map>
#include "bwtree.hpp"
#include <cassert>
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
            if (array[i].key < key) {
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
                switch (nextNode->getType()) {
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
                            nextPID = node1->nodes[res].pid;
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
                    //TODO save for later asynchrounous consolidate
                    needConsolidatePage = nextPID;
                }
                switch (nextNode->getType()) {
                    case PageType::leaf: {
                        auto node1 = static_cast<Leaf<Key, Data> *>(nextNode);
                        if (!doNotSplit && node1->recordCount + deltaNodeCount - removedBySplit > settings.getSplitLimitLeaf() && needSplitPage == NotExistantPID) {
                            if (DEBUG) std::cout << "leaf count" << node1->recordCount << " " << deltaNodeCount << " " << removedBySplit;
                            needSplitPage = nextPID;
                            needSplitPageParent = parent;
                        }
                        auto res = binarySearch<decltype(node1->records)>(node1->records, node1->recordCount, key);
                        if (res < node1->recordCount) {
                            if (node1->records[res].key == key) {
                                return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key,
                                                                     node1->records[res].data,
                                                                     needConsolidatePage, needSplitPage,
                                                                     needSplitPageParent);
                            }
                        } else if (node1->next != NotExistantPID) {
                            doNotSplit = true;
                            nextPID = node1->next;
                            nextNode = nullptr;
                            continue;
                        }
                        return FindDataPageResult<Key, Data>(nextPID, startNode, nullptr, needConsolidatePage, needSplitPage, needSplitPageParent);
                    };
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                        if (node1->record.key == key) {
                            return FindDataPageResult<Key, Data>(nextPID, startNode, nextNode, key, node1->record.data, needConsolidatePage, needSplitPage, needSplitPageParent);
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
                switch (nextNode->getType()) {
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
        DeltaInsert<Key, Data> *newNode = DeltaInsert<Key, Data>::create(res.startNode, KeyValue<Key, Data>(key, record), (res.dataNode != nullptr));
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

        Key Kp, Kq;
        LinkedNode<Key, Data> *newRightNode;
        std::size_t removedElements;
        if (!leaf) {
            static thread_local std::vector<KeyPid<Key, Data>> nodesStatic;
            auto &nodes = nodesStatic;
            nodes.clear();
            PID prev, next;
            bool hadInfinityElement;
            std::tie(prev, next, hadInfinityElement) = getConsolidatedInnerData(startNode, needSplitPage, nodes);

            if (nodes.size() < settings.getSplitLimitInner(0)) {
                return;
            }
            if (DEBUG) std::cout << "inner size: " << nodes.size() << std::endl;
            auto middle = nodes.begin();
            std::advance(middle, (std::distance(nodes.begin(), nodes.end()) / 2));
            std::nth_element(nodes.begin(), middle, nodes.end(), [](const KeyPid<Key, Data> &t1, const KeyPid<Key, Data> &t2) {
                return t1.key < t2.key;
            });

            Kp = middle->key;

            auto newRightInner = Helper<Key, Data>::CreateInnerNodeFromUnsorted(middle + 1, nodes.end(), needSplitPage, next, hadInfinityElement);
            assert(newRightInner->nodeCount > 0);
            Kq = newRightInner->nodes[newRightInner->nodeCount - 1].key;
            removedElements = newRightInner->nodeCount;
            newRightNode = newRightInner;
        } else {
            static thread_local std::vector<KeyValue<Key, Data>> recordsStatic;
            auto &records = recordsStatic;
            records.clear();
            PID prev, next;
            std::tie(prev, next) = getConsolidatedLeafData(startNode, records);
            if (DEBUG) std::cout << "leaf size: " << records.size() << std::endl;
            if (records.size() < settings.getSplitLimitLeaf()) {
                return;
            }
            auto middle = records.begin();
            std::advance(middle, (std::distance(records.begin(), records.end()) / 2));
            Kp = middle->key;

            auto newRightLeaf = Helper<Key, Data>::CreateLeafNodeFromSorted(middle + 1, records.end(), needSplitPage,
                                                                            next);
            assert(newRightLeaf->recordCount > 0);
            Kq = newRightLeaf->records[newRightLeaf->recordCount - 1].key;
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
        }

        if (needSplitPageParent == NotExistantPID) {
            InnerNode<Key, Data> *newRoot = InnerNode<Key, Data>::create(2, NotExistantPID, NotExistantPID);
            newRoot->nodes[0] = KeyPid<Key, Data>(Kp, needSplitPage);
            newRoot->nodes[1] = KeyPid<Key, Data>(std::numeric_limits<Key>::max(), newRightNodePID);
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
            assert(!isLeaf(parentNode));
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

        static thread_local std::vector<KeyValue<Key, Data>> recordsStatic;
        auto &records = recordsStatic;
        records.clear();
        PID prev, next;
        std::tie(prev, next) = getConsolidatedLeafData(startNode, records);
        Leaf<Key, Data> *newNode = Helper<Key, Data>::CreateLeafNodeFromSorted(records.begin(), records.end(), prev,
                                                                               next);

        Node<Key, Data> *previousNode = startNode;

        if (!mapping[pid].compare_exchange_strong(startNode, newNode)) {
            freeNodeSingle<Key, Data>(newNode);
            ++atomicCollisions;
            ++failedLeafConsolidate;
        } else {
            ++successfulLeafConsolidate;
            epoque.markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    std::tuple<PID, PID> Tree<Key, Data>::getConsolidatedLeafData(Node<Key, Data> *node, std::vector<KeyValue<Key, Data>> &records) {
        std::array<KeyValue<Key, Data>, 100> deltaInsertRecords;
        std::size_t deltaInsertRecordsCount = 0;

        std::array<Key, 100> deletedOrUpdatedDeltaKeys;
        std::size_t deletedOrUpdatedDeltaKeysCount = 0;

        Key stopAtKey = std::numeric_limits<Key>::max();
        bool pageSplit = false;
        PID prev, next;
        while (node->getType() != PageType::leaf) {
            switch (node->getType()) {
                case PageType::deltaInsert: {
                    auto node1 = static_cast<DeltaInsert<Key, Data> *>(node);
                    auto &curKey = node1->record.key;
                    if (curKey <= stopAtKey
                            && std::find(deletedOrUpdatedDeltaKeys.begin(), deletedOrUpdatedDeltaKeys.begin() +
                                                                      deletedOrUpdatedDeltaKeysCount, curKey) == deletedOrUpdatedDeltaKeys.begin() +
                                                                                                                 deletedOrUpdatedDeltaKeysCount) {
                        deltaInsertRecords[deltaInsertRecordsCount++] = node1->record;
                        assert(deltaInsertRecordsCount != deltaInsertRecords.size());
                        if (node1->keyExistedBefore) {
                            deletedOrUpdatedDeltaKeys[deletedOrUpdatedDeltaKeysCount++] = curKey;
                            assert(deletedOrUpdatedDeltaKeysCount != deletedOrUpdatedDeltaKeys.size());
                        }
                    }
                    node = node1->origin;
                    continue;
                }
                case PageType::deltaDelete: {
                    auto node1 = static_cast<DeltaDelete<Key, Data> *>(node);
                    auto &curKey = node1->key;
                    if (std::find(deletedOrUpdatedDeltaKeys.begin(), deletedOrUpdatedDeltaKeys.begin() +
                                                               deletedOrUpdatedDeltaKeysCount, curKey) == deletedOrUpdatedDeltaKeys.begin() +
                                                                                                          deletedOrUpdatedDeltaKeysCount) {
                        deletedOrUpdatedDeltaKeys[deletedOrUpdatedDeltaKeysCount++] = curKey;
                        assert(deletedOrUpdatedDeltaKeysCount != deletedOrUpdatedDeltaKeys.size());
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
        std::sort(deltaInsertRecords.begin(), deltaInsertRecords.begin() + deltaInsertRecordsCount, [](const KeyValue<Key, Data> &t1, const KeyValue<Key, Data> &t2) {
            return t1.key < t2.key;
        });
        std::sort(deletedOrUpdatedDeltaKeys.begin(), deletedOrUpdatedDeltaKeys.begin() + deletedOrUpdatedDeltaKeysCount);
        KeyValue<Key, Data> *recordsdata[] = {const_cast<KeyValue<Key, Data> *>(deltaInsertRecords.data()), node1->records};
        std::size_t nextConsideredDeltaKey = 0;
        std::size_t nextrecords[] = {0, 0};
        std::size_t &nextdelta = nextrecords[0];
        std::size_t &nextrecord = nextrecords[1];
        while (nextrecord < node1->recordCount && nextdelta < deltaInsertRecordsCount) {
            const bool hasMoreDeltaKeys = nextConsideredDeltaKey < deletedOrUpdatedDeltaKeysCount;
            const bool advanceDeletedOrUpdatedDeltaKeys =
                    hasMoreDeltaKeys && node1->records[nextrecord].key > deletedOrUpdatedDeltaKeys[nextConsideredDeltaKey];
            if (advanceDeletedOrUpdatedDeltaKeys) {
                nextConsideredDeltaKey++;
                continue;
            }
            bool recordUpdatedOrDeleted =
                    hasMoreDeltaKeys && node1->records[nextrecord].key == deletedOrUpdatedDeltaKeys[nextConsideredDeltaKey];
            if (recordUpdatedOrDeleted) {
                nextrecord++;
                continue;
            }

            std::int8_t choice = (node1->records[nextrecord].key < deltaInsertRecords[nextdelta].key);
            KeyValue<Key, Data> record = recordsdata[choice][nextrecords[choice]];
            ++nextrecords[choice];
            if (record.key <= stopAtKey) {
                records.push_back(record);
            } else {
                nextrecord = node1->recordCount;
                nextdelta = deltaInsertRecordsCount;
                break;
            }
        }
        while (nextrecord < node1->recordCount && node1->records[nextrecord].key <= stopAtKey) {
            if (std::find(deletedOrUpdatedDeltaKeys.begin(),
                          deletedOrUpdatedDeltaKeys.begin() + deletedOrUpdatedDeltaKeysCount, node1->records[nextrecord].key) ==
                    deletedOrUpdatedDeltaKeys.begin() + deletedOrUpdatedDeltaKeysCount) {
                records.push_back(node1->records[nextrecord]);
            }
            ++nextrecord;
        }
        while (nextdelta < deltaInsertRecordsCount) {
            if (deltaInsertRecords[nextdelta].key <= stopAtKey) {
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

        static thread_local std::vector<KeyPid<Key, Data>> nodesStatic;
        auto &nodes = nodesStatic;
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
            ++successfulInnerConsolidate;
            epoque.markForDeletion(previousNode);
        }
    }

    template<typename Key, typename Data>
    std::tuple<PID, PID, bool> Tree<Key, Data>::getConsolidatedInnerData(Node<Key, Data> *node, PID pid, std::vector<KeyPid<Key, Data>> &nodes) {
        std::array<PID, 40> consideredPIDs;
        std::size_t consideredPIDsCount = 0;

        Key stopAtKey = std::numeric_limits<Key>::max();
        bool pageSplit = false;
        PID prev, next;
        bool hadInfinityElement = false;
        while (node != nullptr) {
            switch (node->getType()) {
                case PageType::inner: {
                    auto node1 = static_cast<InnerNode<Key, Data> *>(node);
                    for (std::size_t i = 0; i < node1->nodeCount; ++i) {
                        if (node1->nodes[i].key <= stopAtKey &&
                                std::find(consideredPIDs.begin(), consideredPIDs.begin() + consideredPIDsCount, node1->nodes[i].pid) == consideredPIDs.begin() + consideredPIDsCount) {
                            assert(node1->nodes[i].pid != pid);
                            nodes.push_back(node1->nodes[i]);
                        }
                    }
                    if (!pageSplit && node1->nodes[node1->nodeCount - 1].key == NotExistantPID) {
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
                    if (node1->keyLeft <= stopAtKey && std::find(consideredPIDs.begin(), consideredPIDs.begin() + consideredPIDsCount, node1->oldChild) == consideredPIDs.begin() +
                                                                                                                            consideredPIDsCount) {
                        if (node1->oldChild == pid) {
                            assert(false);
                        }
                        nodes.push_back(KeyPid<Key, Data>(node1->keyLeft, node1->oldChild));
                        consideredPIDs[consideredPIDsCount++] = node1->oldChild;
                        assert(consideredPIDsCount != consideredPIDs.size());
                    }
                    if (node1->keyRight <= stopAtKey && std::find(consideredPIDs.begin(), consideredPIDs.begin() + consideredPIDsCount, node1->child) == consideredPIDs.begin() +
                                                                                                                         consideredPIDsCount) {
                        if (node1->child == pid) {
                            assert(false);
                        }
                        nodes.push_back(KeyPid<Key, Data>(node1->keyRight, node1->child));
                        consideredPIDs[consideredPIDsCount++] = node1->child;
                        assert(consideredPIDsCount != consideredPIDs.size());
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
