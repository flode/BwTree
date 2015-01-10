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
        std::atomic<double> timeForLeafConsolidation{0.0};
        std::atomic<double> timeForInnerConsolidation{0.0};
        std::atomic<double> timeForLeafSplit{0.0};
        std::atomic<double> timeForInnerSplit{0.0};

        Epoque<Key, Data> epoque;

        struct Settings {
            std::size_t ConsolidateLeafPage = 5;
            std::size_t SplitLeafPage = 15;
            std::size_t SplitInnerPage = 10;
        };

        constexpr static Settings settings{};

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

        Leaf<Key, Data> *createConsolidatedLeafPage(Node<Key, Data> *startNode, Key keysGreaterEqualThan = std::numeric_limits<Key>::min());

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

        Tree() {
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


        std::atomic<unsigned long> const &getAtomicCollisions() const {
            return atomicCollisions;
        }

        std::atomic<unsigned long> const &getSuccessfulLeafConsolidate() const {
            return successfulLeafConsolidate;
        }

        std::atomic<unsigned long> const &getSuccessfulInnerConsolidate() const {
            return successfulInnerConsolidate;
        }

        std::atomic<unsigned long> const &getFailedLeafConsolidate() const {
            return failedLeafConsolidate;
        }

        std::atomic<unsigned long> const &getFailedInnerConsolidate() const {
            return failedInnerConsolidate;
        }

        std::atomic<unsigned long> const &getSuccessfulLeafSplit() const {
            return successfulLeafSplit;
        }

        std::atomic<unsigned long> const &getSuccessfulInnerSplit() const {
            return successfulInnerSplit;
        }

        std::atomic<unsigned long> const &getFailedLeafSplit() const {
            return failedLeafSplit;
        }

        std::atomic<unsigned long> const &getFailedInnerSplit() const {
            return failedInnerSplit;
        }

        std::atomic<double> const &getTimeForLeafConsolidation() const {
            return timeForLeafConsolidation;
        }

        std::atomic<double> const &getTimeForInnerConsolidation() const {
            return timeForInnerConsolidation;
        }

        std::atomic<double> const &getTimeForLeafSplit() const {
            return timeForLeafSplit;
        }

        std::atomic<double> const &getTimeForInnerSplit() const {
            return timeForInnerSplit;
        }
    };
}

// Include the cpp file so that the templates can be correctly compiled
#include "bwtree.cpp"

#endif