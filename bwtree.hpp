#ifndef BWTREE_H
#define BWTREE_H

#include <tuple>
#include <vector>
#include <mutex>
#include <atomic>
#include <iostream>

namespace BwTree {

    using PID = std::size_t;
    constexpr PID NotExistantPID = std::numeric_limits<PID>::max();

    enum class PageType : std::int8_t  {
        leaf,
        inner,
        deltaInsert,
        deltaDelete,
        deltaIndex,
        deltaSplit
    };

    template <typename Key, typename Data>
    struct Node {
        PageType type;
    };

    template <typename Key, typename Data>
    struct Leaf : Node<Key,Data> {
        PID prev;
        PID next;
        std::size_t recordCount;
        // has to be last member for dynamic malloc() !!!
        std::tuple<Key, const Data*> records[];
    private:
        Leaf() = delete;
        ~Leaf() = delete;
    };


    template <typename Key, typename Data>
    struct InnerNode : Node<Key,Data> {
        PID prev;
        PID next;
        std::size_t nodeCount;
        // has to be last member for dynamic malloc() !!!
        std::tuple<Key, PID> nodes[];
    private:
        InnerNode() = delete;
        ~InnerNode() = delete;
    };

    template <typename Key, typename Data>
    struct DeltaNode : Node<Key,Data> {
        Node<Key,Data>* origin;
    private:
        DeltaNode() = delete;
        ~DeltaNode() = delete;
    };

    template <typename Key, typename Data>
    struct DeltaInsert : DeltaNode<Key,Data> {
        std::tuple<Key, const Data*> record;
    private:
        DeltaInsert() = delete;
        ~DeltaInsert() = delete;
    };

    template <typename Key, typename Data>
    struct DeltaDelete : DeltaNode<Key,Data> {
        Key key;
    private:
        DeltaDelete() = delete;
        ~DeltaDelete() = delete;
    };

    template <typename Key, typename Data>
    struct DeltaSplit : DeltaNode<Key,Data> {
        Key key;
        PID sidelink;
    private:
        DeltaSplit() = delete;
        ~DeltaSplit() = delete;
    };

    template <typename Key, typename Data>
    struct DeltaIndex : DeltaNode<Key,Data> {
        Key keyLeft;
        Key keyRight;
        PID sidelink;
    private:
        DeltaIndex() = delete;
        ~DeltaIndex() = delete;
    };

    template <typename Key, typename Data>
    Node<Key,Data>* CreateInnerNode(std::size_t size)
    {
        size_t s = sizeof (InnerNode<Key,Data>) - sizeof (InnerNode<Key,Data>::nodes);
        InnerNode<Key,Data> *output = (InnerNode<Key,Data>*) malloc(s + size * sizeof(std::tuple<Key,PID>));
        output->nodeCount = size;
        output->type = PageType::inner;
        return output;
    }

    template <typename Key, typename Data>
    Leaf<Key,Data>* CreateLeaf(std::size_t size)
    {
        size_t s = sizeof (Leaf<Key,Data>) - sizeof (Leaf<Key,Data>::records);
        Leaf<Key,Data> *output = (Leaf<Key,Data>*) malloc(s + size * sizeof(std::tuple<Key,Data*>));
        output->recordCount = size;
        output->type = PageType::leaf;
        return output;
    }

    template <typename Key, typename Data>
    DeltaInsert<Key,Data>* CreateDeltaInsert()
    {
        size_t s = sizeof (DeltaInsert<Key,Data>);
        DeltaInsert<Key,Data> *output = (DeltaInsert<Key,Data>*) malloc(s);
        output->type = PageType::deltaInsert;
        return output;
    }

    template <typename Key, typename Data>
    DeltaDelete<Key,Data>* CreateDeltaDelete()
    {
        size_t s = sizeof (DeltaDelete<Key,Data>);
        DeltaDelete<Key,Data> *output = (DeltaDelete<Key,Data>*) malloc(s);
        output->type = PageType::deltaDelete;
        return output;
    }

    template <typename Key, typename Data>
    DeltaSplit<Key,Data>* CreateDeltaSplit()
    {
        size_t s = sizeof (DeltaSplit<Key,Data>);
        DeltaSplit<Key,Data> *output = (DeltaSplit<Key,Data>*) malloc(s);
        output->type = PageType::deltaSplit;
        return output;
    }

    /* template <typename Key, typename Data>
    class Node {
        // inner node
        std::vector<std::tuple<Key, PID>> nodes;
        //std::size_t subnodeCount;
        // leaf
        std::vector<std::tuple<Key, Data*>> records;
        //std::size_t recordCount;
        PID prev;
        PID next;
    public:
        Node(std::vector<std::tuple<Key, PID>> innerNodes, std::vector<std::tuple<Key, Data*>> leafs) : nodes(innerNodes),records(leafs) {}
        ~Node() {
                delete[] nodes;
                delete[] records;
        }


    private:
        // Will cause compiler error if you misuse this struct
        Node(const Node&);
        void operator=(const Node&);
    };*/
    /*template <typename Key, typename Data, std::size_t sCount> using InnerNode = Node<Key,Data,sCount,0>;
    template <typename Key, typename Data, std::size_t rCount> using LeafNode = Node<Key,Data,0, rCount>;*/


    template <typename Key, typename Data>
    class Tree {
        PID root;
        std::vector<std::atomic<Node<Key,Data>*>> mapping{2048};
        //std::atomic<Node<Key,Data>*> mapping[2048];
        //std::array<std::atomic<Node<Key,Data>*>,2048> mapping{};
        //PID mappingSize = 2048;
        std::atomic<PID> mappingNext{0};
        std::atomic<unsigned long> atomicCollisions{0};

        //std::mutex insertMutex;


        Node<Key,Data>* PIDToNodePtr(PID node) {
            return mapping[node];
        }

        PID newNode(Node<Key,Data>* node) {
            //std::lock_guard<std::mutex> lock(insertMutex);
            PID nextPID = mappingNext;
            while(true) {
                if (nextPID == mapping.size()) {
                    std::cerr << "Mapping table is full, aborting!" << std::endl;
                    exit(1);
                }
                mappingNext.compare_exchange_weak(nextPID, nextPID+1);
                break;
            }
            mapping[nextPID] = node;
            return nextPID;
        }

        /**
        * page id of the leaf node, first node in the chain (corresponds to PID), actual node where the data was found
        */
        std::tuple<PID, Node<Key,Data>*, Node<Key,Data>*> findDataPage(Key key);

        void consolidateLeafPage(PID pid);

        template<typename T>
        static std::size_t binarySearch(T array, std::size_t length, std::size_t tupleIndex, Key key);

    public:

        Tree() {
            Node<Key, Data> *node = CreateLeaf<Key,Data>(0);
            root = newNode(node);
        }

        ~Tree();

        void insert(Key key, const Data * const record);

        void deleteKey(Key key);

        Data* search(Key key);

        unsigned long const getAtomicCollisions() const {
            return atomicCollisions;
        }
    };
}

// Include the cpp file so that the templates can be correctly compiled
#include "bwtree.cpp"
#endif