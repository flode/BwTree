#ifndef BWTREE_H
#define BWTREE_H

#include <tuple>
#include <vector>
#include <mutex>
#include <atomic>
#include <iostream>
#include <stack>
#include <assert.h>

namespace BwTree {

    using PID = std::size_t;
    constexpr PID NotExistantPID = std::numeric_limits<PID>::max();

    enum class PageType : std::int8_t {
        leaf,
        inner,
        deltaInsert,
        deltaDelete,
        deltaIndex,
        deltaSplit,
        deltaSplitInner
    };

    template<typename Key, typename Data>
    struct Node {
        PageType type;
    };

    template<typename Key, typename Data>
    struct Leaf : Node<Key, Data> {
        PID prev;
        PID next;
        std::size_t recordCount;
        // has to be last member for dynamic malloc() !!!
        std::tuple<Key, const Data *> records[];
    private:
        Leaf() = delete;

        ~Leaf() = delete;
    };


    template<typename Key, typename Data>
    struct InnerNode : Node<Key, Data> {
        PID prev;
        PID next;
        std::size_t nodeCount;
        // has to be last member for dynamic malloc() !!!
        std::tuple<Key, PID> nodes[];
    private:
        InnerNode() = delete;

        ~InnerNode() = delete;
    };

    template<typename Key, typename Data>
    struct DeltaNode : Node<Key, Data> {
        Node<Key, Data> *origin;
    private:
        DeltaNode() = delete;

        ~DeltaNode() = delete;
    };

    template<typename Key, typename Data>
    struct DeltaInsert : DeltaNode<Key, Data> {
        std::tuple<Key, const Data *> record;
    private:
        DeltaInsert() = delete;

        ~DeltaInsert() = delete;
    };

    template<typename Key, typename Data>
    struct DeltaDelete : DeltaNode<Key, Data> {
        Key key;
    private:
        DeltaDelete() = delete;

        ~DeltaDelete() = delete;
    };

    template<typename Key, typename Data>
    struct DeltaSplit : DeltaNode<Key, Data> {
        Key key;
        PID sidelink;
    private:
        DeltaSplit() = delete;

        ~DeltaSplit() = delete;
    };

    template<typename Key, typename Data>
    struct DeltaIndex : DeltaNode<Key, Data> {
        Key keyLeft; // greater than
        Key keyRight; // less or equal than
        PID child;
        PID oldChild;
    private:
        DeltaIndex() = delete;

        ~DeltaIndex() = delete;
    };

    template<typename Key, typename Data>
    InnerNode<Key, Data> *CreateInnerNode(std::size_t size, PID next, PID prev) {
        size_t s = sizeof(InnerNode<Key, Data>) - sizeof(InnerNode<Key, Data>::nodes);
        InnerNode<Key, Data> *output = (InnerNode<Key, Data> *) malloc(s + size * sizeof(std::tuple<Key, PID>));
        output->nodeCount = size;
        output->type = PageType::inner;
        output->next = next;
        output->prev = prev;
        return output;
    }

    template<typename Key, typename Data>
    Leaf<Key, Data> *CreateLeaf(std::size_t size, PID next, PID prev) {
        size_t s = sizeof(Leaf<Key, Data>) - sizeof(Leaf<Key, Data>::records);
        Leaf<Key, Data> *output = (Leaf<Key, Data> *) malloc(s + size * sizeof(std::tuple<Key, const Data *>));
        output->recordCount = size;
        output->type = PageType::leaf;
        output->next = next;
        output->prev = prev;
        return output;
    }

    template<typename Key, typename Data>
    DeltaInsert<Key, Data> *CreateDeltaInsert(Node<Key, Data> *origin, std::tuple<Key, const Data *> record) {
        size_t s = sizeof(DeltaInsert<Key, Data>);
        DeltaInsert<Key, Data> *output = (DeltaInsert<Key, Data> *) malloc(s);
        output->type = PageType::deltaInsert;
        output->origin = origin;
        output->record = record;
        return output;
    }

    template<typename Key, typename Data>
    DeltaDelete<Key, Data> *CreateDeltaDelete(Node<Key, Data> *origin, Key key) {
        size_t s = sizeof(DeltaDelete<Key, Data>);
        DeltaDelete<Key, Data> *output = (DeltaDelete<Key, Data> *) malloc(s);
        output->type = PageType::deltaDelete;
        output->origin = origin;
        output->key = key;
        return output;
    }

    template<typename Key, typename Data>
    DeltaSplit<Key, Data> *CreateDeltaSplit(Node<Key, Data> *origin, Key splitKey, PID sidelink) {
        size_t s = sizeof(DeltaSplit<Key, Data>);
        DeltaSplit<Key, Data> *output = (DeltaSplit<Key, Data> *) malloc(s);
        output->type = PageType::deltaSplit;
        output->origin = origin;
        output->key = splitKey;
        output->sidelink = sidelink;
        return output;
    }

    template<typename Key, typename Data>
    DeltaSplit<Key, Data> *CreateDeltaSplitInner(Node<Key, Data> *origin, Key splitKey, PID sidelink) {
        DeltaSplit<Key, Data> *output = CreateDeltaSplit(origin, splitKey, sidelink);
        output->type = PageType::deltaSplitInner;
        return output;
    }

    template<typename Key, typename Data>
    DeltaIndex<Key, Data> *CreateDeltaIndex(Node<Key, Data> *origin, Key splitKeyLeft, Key splitKeyRight, PID child, PID oldChild) {
        size_t s = sizeof(DeltaIndex<Key, Data>);
        DeltaIndex<Key, Data> *output = (DeltaIndex<Key, Data> *) malloc(s);
        output->type = PageType::deltaIndex;
        output->origin = origin;
        output->keyLeft = splitKeyLeft;
        output->keyRight = splitKeyRight;
        output->child = child;
        output->oldChild = oldChild;
        return output;
    }

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
    class Epoque {
        static const std::size_t epoquescount{40000};
        std::atomic<unsigned> epoques[epoquescount];
        std::array<Node<Key, Data> *, 10> deletedNodes[epoquescount];
        std::atomic<std::size_t> deleteNodeNext[epoquescount];
        std::atomic<unsigned> oldestEpoque{0};
        std::atomic<unsigned> newestEpoque{0};

        std::mutex mutex;

    public:
        unsigned enterEpoque();

        void leaveEpoque(unsigned e);

        void markForDeletion(Node<Key, Data> *);
    };


    template<typename Key, typename Data>
    class Tree {
        /**
        * Special Invariant:
        * - Leaf nodes always contain special infinity value at the right end for the last pointer
        */
        PID root;
        std::vector<std::atomic<Node<Key, Data> *>> mapping{100000};
        //std::atomic<Node<Key,Data>*> mapping[2048];
        //std::array<std::atomic<Node<Key,Data>*>,2048> mapping{};
        //PID mappingSize = 2048;
        std::atomic<PID> mappingNext{0};
        std::atomic<unsigned long> atomicCollisions{0};
        std::atomic<unsigned long> successfulConsolidate{0};
        std::atomic<unsigned long> failedConsolidate{0};
        std::atomic<unsigned long> successfulSplit{0};
        std::atomic<unsigned long> failedSplit{0};

        Epoque<Key, Data> epoque;

        struct Settings {
            std::size_t ConsolidateLeafPage = 8;
            std::size_t SplitLeafPage = 14;
        };

        constexpr static Settings settings{};

        //std::mutex insertMutex;

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

        InnerNode<Key, Data> *createConsolidatedInnerPage(Node<Key, Data> *startNode, Key keysGreaterEqualThan = std::numeric_limits<Key>::min());

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
                    assert(false); // not implemented
                case PageType::leaf:
                case PageType::deltaDelete: /* fallthrough */
                case PageType::deltaSplit: /* fallthrough */
                case PageType::deltaInsert:
                    splitLeafPage(pid, node, std::move(stack));
                    break;
            }
        }

        void splitLeafPage(PID pid, Node<Key, Data> *node, std::stack<PID> &&stack);


        template<typename T>
        static size_t binarySearch(T array, std::size_t length, Key key);

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

        unsigned long const getAtomicCollisions() const {
            return atomicCollisions;
        }

        unsigned long const getSuccessfulConsolidate() const {
            return successfulConsolidate;
        }

        unsigned long const getFailedConsolidate() const {
            return failedConsolidate;
        }

        unsigned long const getSuccessfulSplit() const {
            return successfulSplit;
        }

        unsigned long const getFailedSplit() const {
            return failedSplit;
        }
    };
}

// Include the cpp file so that the templates can be correctly compiled
#include "bwtree.cpp"

#endif