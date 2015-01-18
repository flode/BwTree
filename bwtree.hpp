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

#include "nodes.hpp"
#include "epoque.hpp"

namespace BwTree {

    template<typename Key, typename Data>
    struct FindDataPageResult {
        PID pid;
        Node<Key, Data> *startNode;
        Node<Key, Data> *dataNode;
        Key key;
        const Data *data;
        std::stack<PID> needConsolidatePage;
        std::stack<PID> needSplitPage;
        std::stack<PID> parentNodes;


        FindDataPageResult(PID pid, Node<Key, Data> *startNode, Node<Key, Data> *dataNode, std::stack<PID> const &needConsolidatePage, std::stack<PID> const &needSplitPage, std::stack<PID> const &parentNodes)
                : pid(pid),
                  startNode(startNode),
                  dataNode(dataNode),
                  needConsolidatePage(needConsolidatePage),
                  needSplitPage(needSplitPage),
                  parentNodes(parentNodes) {
        }

        FindDataPageResult(PID pid, Node<Key, Data> *startNode, Node<Key, Data> *dataNode, Key key, Data const *data, std::stack<PID> const &&needConsolidatePage, std::stack<PID> const &&needSplitPage, std::stack<PID> const &&parentNodes)
                : pid(pid),
                  startNode(startNode),
                  dataNode(dataNode),
                  key(key),
                  data(data),
                  needConsolidatePage(needConsolidatePage),
                  needSplitPage(needSplitPage),
                  parentNodes(parentNodes) {
        }
    };


    struct Settings {
        Settings(size_t splitLeaf, std::vector<size_t> const &splitInner, size_t consolidateLeaf, std::vector<size_t> const &consolidateInner)
                : splitLeaf(splitLeaf),
                  splitInner(splitInner),
                  consolidateLeaf(consolidateLeaf),
                  consolidateInner(consolidateInner) {
        }

        std::size_t splitLeaf;
        std::size_t getSplitLimitLeaf() const {
            return splitLeaf;
        }

        std::vector<std::size_t> splitInner;
        std::size_t getSplitLimitInner(unsigned level) const {
            return level < splitInner.size() ? splitInner[level] : splitInner[splitInner.size()-1];
        }

        std::size_t consolidateLeaf;
        std::size_t getConsolidateLimitLeaf() const {
            return consolidateLeaf;
        }

        std::vector<std::size_t> consolidateInner;
        std::size_t getConsolidateLimitInner(unsigned level) const {
            return level < consolidateInner.size() ? consolidateInner[level] : consolidateInner[consolidateInner.size()-1];
        }
    };

    template<typename Key, typename Data>
    class Tree {
        /**
        * Special Invariant:
        * - Leaf nodes always contain special infinity value at the right end for the last pointer
        */
        std::atomic<PID> root;
        std::vector<std::atomic<Node<Key, Data> *>> mapping{100000};
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
        std::atomic<long> timeForLeafConsolidation{0};
        std::atomic<long> timeForInnerConsolidation{0};
        std::atomic<long> timeForLeafSplit{0};
        std::atomic<long> timeForInnerSplit{0};

        Epoque<Key, Data> epoque;

        const Settings &settings;

        //std::mutex insertMutex;
//        std::array<Node<Key, Data> *, 100000> deletedNodes;
//        std::atomic<std::size_t> deleteNodeNext{0};

        Node<Key, Data> *PIDToNodePtr(PID node) {
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

        void consolidatePage(std::stack<PID> &&stack) {//TODO add to a list
            PID pid = stack.top();
            stack.pop();
            Node<Key, Data> *node = mapping[pid];
            switch (node->type) {
                case PageType::inner: /* fallthrough */
                case PageType::deltaSplitInner: /* fallthrough */
                case PageType::deltaIndex:
                    consolidateInnerPage(pid, node);
                    break;
                case PageType::leaf:
                case PageType::deltaDelete: /* fallthrough */
                case PageType::deltaSplit: /* fallthrough */
                case PageType::deltaInsert:
                    auto a = node->type;
                    consolidateLeafPage(pid, node);
                    break;
            }
        }

        void consolidateInnerPage(PID pid, Node<Key, Data> *node);

        std::tuple<PID, PID, bool> getConsolidatedInnerData(Node<Key, Data> *node, std::vector<std::tuple<Key, PID>>& returnNodes);
        InnerNode<Key, Data> *createConsolidatedInnerPage(Node<Key, Data> *startNode);

        void consolidateLeafPage(PID pid, Node<Key, Data> *node);

        std::tuple<PID, PID> getConsolidatedLeafData(Node<Key, Data> *node, std::vector<std::tuple<Key, const Data*>>& returnNodes);
        Leaf<Key, Data> *createConsolidatedLeafPage(Node<Key, Data> *startNode);

        void splitPage(std::stack<PID> &&stack) {
            PID pid = stack.top();
            stack.pop();
            Node<Key, Data> *node = mapping[pid];
            switch (node->type) {
                case PageType::inner: /* fallthrough */
                case PageType::deltaSplitInner: /* fallthrough */
                case PageType::deltaIndex:
                    splitPage(pid, false, node, std::move(stack));
                    break;
                    assert(false); // not implemented
                case PageType::leaf:
                case PageType::deltaDelete: /* fallthrough */
                case PageType::deltaSplit: /* fallthrough */
                case PageType::deltaInsert:
                    splitPage(pid, true, node, std::move(stack));
                    break;
            }
        }

        void splitPage(PID pid, bool leaf, Node<Key, Data> *node, std::stack<PID> &&stack);


        template<typename T>
        static size_t binarySearch(T array, std::size_t length, Key key);

        std::default_random_engine d;
        std::uniform_int_distribution<int> rand{0,100};

    public:

        Tree(Settings &settings) : settings(settings) {
            Node<Key, Data> *datanode = CreateLeaf<Key, Data>(0, NotExistantPID, NotExistantPID);
            PID dataNodePID = newNode(datanode);
            InnerNode<Key, Data> *innerNode = CreateInnerNode<Key, Data>(1, NotExistantPID, NotExistantPID);
            innerNode->nodes[0] = std::make_tuple(std::numeric_limits<Key>::max(), dataNodePID);
            root = newNode(innerNode);
        }

        ~Tree();

        void insert(Key key, const Data *const record);

        void deleteKey(Key key);

        Data *search(Key key);


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

        long getTimeForLeafConsolidation() const {
            return timeForLeafConsolidation;
        }

        long getTimeForInnerConsolidation() const {
            return timeForInnerConsolidation;
        }

        long getTimeForLeafSplit() const {
            return timeForLeafSplit;
        }

        long getTimeForInnerSplit() const {
            return timeForInnerSplit;
        }
    };
}

// Include the cpp file so that the templates can be correctly compiled
#include "bwtree.cpp"

#endif