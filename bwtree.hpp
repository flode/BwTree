#ifndef BWTREE_H
#define BWTREE_H

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


        FindDataPageResult(PID const pid, Node<Key, Data> *startNode, Node<Key, Data> const *dataNode, PID const needConsolidatePage, PID const needSplitPage, PID const needSplitPageParent);

        FindDataPageResult(PID const pid, Node<Key, Data> *startNode, Node<Key, Data> const *dataNode, Key const key, Data const *data, PID const needConsolidatePage, PID const needSplitPage, PID const needSplitPageParent);
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

        Epoche<Key, Data> epoque{64};

        const Settings &settings;

        Node<Key, Data> *PIDToNodePtr(const PID node) {
            return mapping[node];
        }

        PID newNode(Node<Key, Data> *node) {
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

        void consolidatePage(const PID pid, ThreadInfo<Key, Data> &threadInfo) {
            Node<Key, Data> *node = PIDToNodePtr(pid);
            if (isLeaf(node)) {
                consolidateLeafPage(pid, node, threadInfo);
            } else {
                consolidateInnerPage(pid, node, threadInfo);
            }
        }

        void consolidateInnerPage(PID pid, Node<Key, Data> *startNode, ThreadInfo<Key, Data> &threadInfo);

        std::tuple<PID, PID, bool> getConsolidatedInnerData(Node<Key, Data> *node, PID pid, std::vector<KeyPid<Key, Data>> &returnNodes);

        void consolidateLeafPage(PID pid, Node<Key, Data> *startNode, ThreadInfo<Key, Data> &threadInfo);

        std::tuple<PID, PID> getConsolidatedLeafData(Node<Key, Data> *node, std::vector<KeyValue<Key, Data>> &returnNodes);

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
            innerNode->nodes[0] = KeyPid<Key, Data>(std::numeric_limits<Key>::max(), dataNodePID);
            root.store(newNode(innerNode));
        }

        ~Tree();

        void insert(Key key, const Data *const record, ThreadInfo<Key, Data> &threadInfo);

        void deleteKey(Key key, ThreadInfo<Key, Data> &threadInfo);

        Data *search(Key key, ThreadInfo<Key, Data> &threadInfo);

        ThreadInfo<Key, Data> getThreadInfo();

        /**
        * has to be called when no further work is to be done the next time so that the epoques can be freed.
        */
        void threadFinishedWithTree() {
            //TODO EnterEpoque<Key, Data>::threadFinishedWithTree(epoque);
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

    };
}
#endif
