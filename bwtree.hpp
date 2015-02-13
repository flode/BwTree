#ifndef BWTREE_H
#define BWTREE_H

#undef NDEBUG

#include <tuple>
#include <vector>
#include <mutex>
#include <atomic>
#include <random>
#include <iostream>
#include <stack>
#include <assert.h>
#include <sys/wait.h>

#include "nodes.hpp"
#include "epoque.hpp"

namespace BwTree {

    template<typename Key, typename Data>
    struct FindDataPageResult {
        const PID pid;
        Node<Key, Data> *startNode;
        const Node<Key, Data> *const dataNode;
        const Key key = NotExistantPID;
        const Data *const data = nullptr;
        const PID needConsolidatePage;
        const PID needSplitPage;
        const PID needSplitPageParent;


        FindDataPageResult(PID const pid, Node<Key, Data> *startNode, Node<Key, Data> const *dataNode, PID const needConsolidatePage, PID const needSplitPage, PID const needSplitPageParent)
                : pid(pid),
                  startNode(startNode),
                  dataNode(dataNode),
                  needConsolidatePage(needConsolidatePage),
                  needSplitPage(needSplitPage),
                  needSplitPageParent(needSplitPageParent) {
        }


        FindDataPageResult(PID const pid, Node<Key, Data> *startNode, Node<Key, Data> const *dataNode, Key const key, Data const *data, PID const needConsolidatePage, PID const needSplitPage, PID const needSplitPageParent)
                : pid(pid),
                  startNode(startNode),
                  dataNode(dataNode),
                  key(key),
                  data(data),
                  needConsolidatePage(needConsolidatePage),
                  needSplitPage(needSplitPage),
                  needSplitPageParent(needSplitPageParent) {
        }
    };


    struct Settings {
        std::string name;

        Settings(std::string name, size_t splitLeaf, std::vector<size_t> const &splitInner, size_t consolidateLeaf, std::vector<size_t> const &consolidateInner)
                : name(name), splitLeaf(splitLeaf),
                  splitInner(splitInner),
                  consolidateLeaf(consolidateLeaf),
                  consolidateInner(consolidateInner) {
        }

        std::size_t splitLeaf;

        const std::size_t &getSplitLimitLeaf() const {
            return splitLeaf;
        }

        std::vector<std::size_t> splitInner;

        const std::size_t &getSplitLimitInner(unsigned level) const {
            return level < splitInner.size() ? splitInner[level] : splitInner[splitInner.size() - 1];
        }

        std::size_t consolidateLeaf;

        const std::size_t &getConsolidateLimitLeaf() const {
            return consolidateLeaf;
        }

        std::vector<std::size_t> consolidateInner;

        const std::size_t &getConsolidateLimitInner(unsigned level) const {
            return level < consolidateInner.size() ? consolidateInner[level] : consolidateInner[consolidateInner.size() - 1];
        }

        const std::string &getName() const {
            return name;
        }
    };

    template<typename Key, typename Data>
    class Tree {
        static constexpr bool DEBUG = false;
        /**
        * Special Invariant:
        * - Leaf nodes always contain special infinity value at the right end for the last pointer
        */
        std::atomic<PID> root;
        std::vector<std::atomic<Node<Key, Data> *>> mapping{4194304};
        //std::atomic<Node<Key,Data>*> mapping[2048];
        //std::array<std::atomic<Node<Key,Data>*>,2048> mapping{};
        //PID mappingSize = 2048;
        std::atomic<PID> mappingNext{0};
        std::atomic<unsigned long> atomicCollisions{0};
        std::atomic<unsigned long> successfulLeafConsolidate{0};
        std::atomic<unsigned long> successfulInnerConsolidate{0};
        std::atomic<unsigned long> failedLeafConsolidate{0};
        std::atomic<unsigned long> failedInnerConsolidate{0};
        std::atomic<unsigned long> successfulLeafSplit{0};
        std::atomic<unsigned long> successfulInnerSplit{0};
        std::atomic<unsigned long> failedLeafSplit{0};
        std::atomic<unsigned long> failedInnerSplit{0};

        Epoque<Key, Data> epoque;


        const Settings &settings;

        //std::mutex insertMutex;
//        std::array<Node<Key, Data> *, 100000> deletedNodes;
//        std::atomic<std::size_t> deleteNodeNext{0};

        Node<Key, Data> *PIDToNodePtr(const PID node) {
            return mapping[node];
        }

        PID newNode(Node<Key, Data> *node) {
            //std::lock_guard<std::mutex> lock(insertMutex);
            PID nextPID = mappingNext++;
            if (nextPID >= mapping.size()) {
                std::cerr << "Mapping table is full, aborting!" << std::endl;
                exit(1);
            }
            mapping[nextPID] = node;
            return nextPID;
        }

        /**
        * page id of the leaf node, first node in the chain (corresponds to PID), actual node where the data was found
        */
        FindDataPageResult<Key, Data> findDataPage(Key key);

        void consolidatePage(const PID pid) {//TODO add to a list
            Node<Key, Data> *node = PIDToNodePtr(pid);
            if (isLeaf(node)) {
                consolidateLeafPage(pid, node);
            } else {
                consolidateInnerPage(pid, node);
            }
        }

        void consolidateInnerPage(PID pid, Node<Key, Data> *startNode);

        std::tuple<PID, PID, bool> getConsolidatedInnerData(Node<Key, Data> *node, PID pid, std::vector<std::tuple<Key, PID>> &returnNodes);

        void consolidateLeafPage(PID pid, Node<Key, Data> *startNode);

        std::tuple<PID, PID> getConsolidatedLeafData(Node<Key, Data> *node, std::vector<std::tuple<Key, const Data *>> &returnNodes);

        void splitPage(const PID needSplitPage, const PID needSplitPageParent);

        std::tuple<PID, Node<Key, Data> *> findInnerNodeOnLevel(PID pid, Key key);

        bool isLeaf(Node<Key, Data> *node) {
            switch (node->getType()) {
                case PageType::inner: /* fallthrough */
                case PageType::deltaSplitInner: /* fallthrough */
                case PageType::deltaIndex:
                    return false;
                case PageType::leaf:
                case PageType::deltaDelete: /* fallthrough */
                case PageType::deltaSplit: /* fallthrough */
                case PageType::deltaInsert:
                    return true;
            }
            assert(false);
            return false;
        }

        template<typename T>
        static size_t binarySearch(T array, std::size_t length, Key key);

        std::default_random_engine d;
        std::uniform_int_distribution<int> rand{0, 100};

    public:

        Tree(Settings &settings) : settings(settings) {
            Node<Key, Data> *datanode = Leaf<Key, Data>::create(0, NotExistantPID, NotExistantPID);
            PID dataNodePID = newNode(datanode);
            InnerNode<Key, Data> *innerNode = InnerNode<Key, Data>::create(1, NotExistantPID, NotExistantPID);
            innerNode->nodes[0] = std::make_tuple(std::numeric_limits<Key>::max(), dataNodePID);
            root.store(newNode(innerNode));
        }

        ~Tree();

        void insert(Key key, const Data *const record);

        void deleteKey(Key key);

        Data *search(Key key);

        /**
        * has to be called when no further work is to be done the next time so that the epoques can be freed.
        */
        void threadFinishedWithTree() {
            EnterEpoque<Key, Data>::threadFinishedWithTree(epoque);
        }


        unsigned long getAtomicCollisions() const {
            return atomicCollisions;
        }

        unsigned long getSuccessfulLeafConsolidate() const {
            return successfulLeafConsolidate;
        }

        unsigned long getSuccessfulInnerConsolidate() const {
            return successfulInnerConsolidate;
        }

        unsigned long getFailedLeafConsolidate() const {
            return failedLeafConsolidate;
        }

        unsigned long getFailedInnerConsolidate() const {
            return failedInnerConsolidate;
        }

        unsigned long getSuccessfulLeafSplit() const {
            return successfulLeafSplit;
        }

        unsigned long getSuccessfulInnerSplit() const {
            return successfulInnerSplit;
        }

        unsigned long getFailedLeafSplit() const {
            return failedLeafSplit;
        }

        unsigned long getFailedInnerSplit() const {
            return failedInnerSplit;
        }

//        void testAutomaton() {
//            PID nextPID = 0;
//            Data lastvalue = 0;
//            while (nextPID != NotExistantPID) {
//                //std::cout << std::endl;
//
//                Node<Key, Data> *nextNode = PIDToNodePtr(nextPID);
//                /*if (nextNode->type != PageType::leaf) {
//                    consolidateLeafPage(nextPID, nextNode);
//                    nextNode = PIDToNodePtr(nextPID);
//                }*/
//
//                PID splitPID = NotExistantPID;
//                Key splitKey = std::numeric_limits<Key>::max();
//                while (nextNode != nullptr) {
//                    switch (nextNode->type) {
//                        /* case PageType::deltaIndex: {
//                        auto node1 = static_cast<DeltaIndex<Key, Data> *>(nextNode);
//                        if (key > node1->keyLeft && key <= node1->keyRight) {
//                            level++;
//                            nextPID = node1->child;
//                            nextNode = nullptr;
//                            break;
//                        } else {
//                            deltaNodeCount++;
//                            nextNode = node1->origin;
//                            continue;
//                        }
//                    };
//                    case PageType::inner: {
//                        auto node1 = static_cast<InnerNode<Key, Data> *>(nextNode);
//                        if (node1->nodeCount + deltaNodeCount - removedBySplit > settings.SplitInnerPage && needSplitPage.size() == 0 && this->rand(this->d) - level < 10) {
//                            if (DEBUG) std::cout << "inner count" << node1->nodeCount << " " << deltaNodeCount << " " << removedBySplit;
//                            needSplitPage = parentNodes;
//                        }
//                        auto res = binarySearch<decltype(node1->nodes)>(node1->nodes, node1->nodeCount, key);
//                        if (res == node1->nodeCount && node1->next != NotExistantPID) {
//                            parentNodes.pop();// to keep correct parent history upward
//                            nextPID = node1->next;
//                        } else {
//                            level++;
//                            nextPID = std::get<1>(node1->nodes[res]);
//                        }
//                        nextNode = nullptr;
//                        break;
//                    };*/
//                        case PageType::leaf: {
//                            auto node1 = static_cast<Leaf<Key, Data> *>(nextNode);
//
//                            for (std::size_t i = 0; i < node1->recordCount; ++i) {
//                                if (std::get<0>(node1->records[i]) <= splitKey) {
//                                    if (lastvalue > std::get<0>(node1->records[i])) {
//                                        auto b = std::get<0>(node1->records[i]);
//                                        std::cerr << b;
//                                        std::cerr << std::endl;
//                                        //exit(-1);
//                                        assert(false);
//                                    }
//                                    lastvalue = std::get<0>(node1->records[i]);
//                                    //std::cout << lastvalue << " ";
//                                }
//                            }
//                            if (splitPID != NotExistantPID) {
//                                nextPID = splitPID;
//                            } else {
//                                nextPID = node1->next;
//                            }
//                            nextNode = nullptr;
//                            break;
//                        };
//                        case PageType::deltaInsert: {
//                            auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
//                            /*if (std::get<0>(node1->record) <= splitKey) {
//                                if (lastvalue > std::get<0>(node1->record)) {
//                                    assert(false);
//                                }
//                                lastvalue = std::get<0>(node1->record);
//                                //std::cout << std::get<0>(node1->record) << " ";
//                            }*/
//                            nextNode = node1->origin;
//                            assert(nextNode != nullptr);
//                            continue;
//                        };
//                        case PageType::deltaDelete: {
//                            auto node1 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
//                            //remember TODO
//                            nextNode = node1->origin;
//                            assert(nextNode != nullptr);
//                            continue;
//                        };
//                        case PageType::deltaSplitInner:
//                        case PageType::deltaSplit: {
//                            auto node1 = static_cast<DeltaSplit<Key, Data> *>(nextNode);
//                            if (splitPID == NotExistantPID) {
//                                splitKey = node1->key;
//                                splitPID = node1->sidelink;
//                            }
//                            nextNode = node1->origin;
//                            assert(nextNode != nullptr);
//                            continue;
//                        };
//                        default: {
//                            assert(false); // not implemented
//                        }
//                    }
//                    nextNode = nullptr;
//                }
//            }
//        }
    };
}

// Include the cpp file so that the templates can be correctly compiled
#include "bwtree.cpp"

#endif
