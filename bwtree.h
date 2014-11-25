#ifndef BWTREE_H
#define BWTREE_H

#include <tuple>
#include <vector>
#include <mutex>
#include <atomic>

namespace BwTree {

    using PID = std::size_t;
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
        std::tuple<Key, Data*> records[];
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
        InnerNode() {}
        ~InnerNode() {}
    };

    template <typename Key, typename Data>
    struct DeltaInsert : Node<Key,Data> {
        std::tuple<Key, Data*> record;
        Node<Key,Data>* origin;
    private:
        DeltaInsert() {}
        ~DeltaInsert() {}
    };

    template <typename Key, typename Data>
    struct DeltaDelete : Node<Key,Data> {
        Key key;
        Node<Key,Data>* origin;
    private:
        DeltaDelete() {}
        ~DeltaDelete() {}
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
    Node<Key,Data>* CreateLeaf(std::size_t size)
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
        //std::mutex insertMutex;


        /*Node<Key,Data>* PIDToNodePtr(PID node) {
            return mapping[node];
        }*/

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

//        void exchangePID(PID node, Node<Key,Data>* oldNode, Node<Key,Data>* newNode) {
  //      }

    public:

        Tree() {
//            for(auto&x:mapping)
//                std::atomic_init(&x,nullptr);
            Node<Key, Data> *node = CreateLeaf<Key,Data>(0);//CreateInnerNode<Key,Data>(0);
            //new Node<Key, Data>({},{});
            root = newNode(node);
        }

        ~Tree() {
            // TODO free()
        }

        void insert(Key key, Data *record) {
            auto nextNode = root;
            while (nextNode != std::numeric_limits<PID>::max()) {
                Node<Key, Data> *node = mapping[nextNode];
                switch (node->type) {
                    case PageType::inner:
                        break;
                    case PageType::deltaInsert:
                    case PageType::leaf: {
                        DeltaInsert<Key, Data> *node1 = CreateDeltaInsert<Key, Data>();
                        std::get<0>(node1->record) = key;
                        std::get<1>(node1->record) = record;
                        node1->origin = node;
                        if (!mapping[nextNode].compare_exchange_weak(node, node1)) {
                            free(node1);
                            nextNode = root;
                            continue;
                        }
                        return;
                    }
/*                    case PageType::deltaDelete: {
                        auto node2 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                        nextNode = node2->origin;
                        continue;
                    }*/

                    default: {
                    }
                        switch (node->type) {
                            //TODO the ones that are less likely to occur
                        }
                }
            }
        }

        void deleteKey(Key key) {
            auto nextNode = root;
            while (nextNode != std::numeric_limits<PID>::max()) {
                Node<Key, Data> *node = mapping[nextNode];
                switch (node->type) {
                    case PageType::inner:
                        break;
                    case PageType::deltaInsert:
                    case PageType::leaf: {
                        DeltaDelete<Key, Data> *node1 = CreateDeltaDelete<Key, Data>();
                        node1->key = key;
                        node1->origin = node;
                        if (!mapping[nextNode].compare_exchange_weak(node, node1)) {
                            free(node1);
                            nextNode = root;
                            continue;
                        }
                        return;
                    }
/*                    case PageType::deltaDelete: {
                        auto node2 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                        nextNode = node2->origin;
                        continue;
                    }*/

                    default: {
                    }
                        switch (node->type) {
                            //TODO the ones that are less likely to occur
                        }
                }
            }
        }

        Data* search(Key key) {
            auto nextPID = root;
            while (nextPID != std::numeric_limits<PID>::max()) {
                Node<Key, Data> *nextNode = mapping[nextPID];
                while (nextNode != nullptr) {
                    switch (nextNode->type) {
                        case PageType::inner:
                            break;
                        case PageType::leaf: {
                            auto node2 = static_cast<Leaf<Key, Data> *>(nextNode);
                            for (int i = 0; i < node2->recordCount; ++i) {
                                if (std::get<0>(node2->records[i]) == key) {
                                    return std::get<1>(node2->records[i]);
                                }
                                nextPID = std::numeric_limits<PID>::max();
                                continue;
                            }
                            return nullptr;
                        }
                        case PageType::deltaInsert: {
                            auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                            if (std::get<0>(node1->record) == key) {
                                return std::get<1>(node1->record);
                            } else {
                                nextNode = node1->origin;
                                continue;
                            }
                        }
                        case PageType::deltaDelete: {
                            auto node2 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                            if (node2->key == key) {
                                return nullptr;
                            }
                            nextNode = node2->origin;
                            continue;
                        }
                        default: {
                        }
                    }
                        nextNode = nullptr;
                    }
            }
            return nullptr;
        }
    };
}


#endif