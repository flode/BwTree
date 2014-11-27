#include "bwtree.hpp"
namespace BwTree {

    template<typename Key, typename Data>
    Data *Tree<Key, Data>::search(Key key) {
        PID pid = findDataPage(key);
        if (pid == std::numeric_limits<PID>::max()) {
            return nullptr;
        }
        Node<Key, Data> *node = mapping[pid];
        while (node != nullptr) {
            switch (node->type) {
                case PageType::leaf: {
                    auto node2 = static_cast<Leaf<Key, Data> *>(node);
                    for (int i = 0; i < node2->recordCount; ++i) {
                        if (std::get<0>(node2->records[i]) == key) { //TODO with binary search
                            return std::get<1>(node2->records[i]);
                        }
                    }
                    return nullptr;
                }
                case PageType::deltaInsert: {
                    auto node1 = static_cast<DeltaInsert<Key, Data> *>(node);
                    if (std::get<0>(node1->record) == key) {
                        return std::get<1>(node1->record);
                    } else {
                        node = node1->origin;
                        continue;
                    }
                }
                case PageType::deltaDelete: {
                    auto node2 = static_cast<DeltaDelete<Key, Data> *>(node);
                    if (node2->key == key) {
                        return nullptr;
                    }
                    node = node2->origin;
                    continue;
                }
                case PageType::deltaSplit: {
                    auto node1 = static_cast<DeltaSplit<Key, Data> *>(node);
                    /*if (key > node1->key) {
                        node = node1->sidelink;
                        continue;
                    }*/
                    node = node1->origin;
                    continue;
                };
                default: {
                }
            }
            node = nullptr;
        }

        return nullptr;
    }


    template<typename Key, typename Data>
    PID Tree<Key, Data>::findDataPage(Key key) {
        auto nextPID = root;
        while (nextPID != std::numeric_limits<PID>::max()) {
            Node<Key, Data> *nextNode = mapping[nextPID];
            while (nextNode != nullptr) {
                switch (nextNode->type) {
                    case PageType::deltaIndex:
                    case PageType::inner:{
                        auto node1 = static_cast<InnerNode<Key,Data>*>(nextNode);
                        for (int i = 0; i < node1->nodeCount; ++i) {
                            if (std::get<0>(node1->nodes[i]) < key){
                                return std::get<1>(node1->nodes[i]);
                            }
                        }
                        // TODO with binary search
                        return std::numeric_limits<PID>::max();
                    };
                    case PageType::leaf:
                    case PageType::deltaInsert: {
                        auto node1 = static_cast<DeltaInsert<Key, Data> *>(nextNode);
                        if (std::get<0>(node1->record) == key) {
                            return nextPID;
                        }
                        nextNode = node1->origin;
                        continue;
                    };
                    case PageType::deltaDelete: {
                        auto node1 = static_cast<DeltaDelete<Key, Data> *>(nextNode);
                        if (node1->key == key) {
                            return std::numeric_limits<PID>::max();
                        }
                        nextNode = node1->origin;
                        continue;
                    };
                    case PageType::deltaSplit: {
                        auto node1 = static_cast<DeltaSplit<Key, Data> *>(nextNode);
                        if (key > node1->key) {
                            nextPID = node1->sidelink;
                            continue;
                        }
                        nextNode = node1->origin;
                        continue;
                        return nextPID;//TODO traverse further? - yes
                    };
                    default: {
                    }
                }
                nextNode = nullptr;
            }
        }
        return std::numeric_limits<PID>::max();
    }


    template<typename Key, typename Data>
    void Tree<Key, Data>::insert(Key key, Data *record) {
        auto nextNode = root;
        while (nextNode != std::numeric_limits<PID>::max()) {
            Node<Key, Data> *node = mapping[nextNode];
            switch (node->type) {
                case PageType::inner:
                    break;
                case PageType::deltaInsert:
                case PageType::deltaDelete:
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

    template<typename Key, typename Data>
    void Tree<Key, Data>::deleteKey(Key key) {
        PID nextNode = findDataPage(key);
        Node<Key, Data> *node = mapping[nextNode];
        switch (node->type) {
            case PageType::deltaDelete:
            case PageType::deltaInsert:
            case PageType::leaf: {
                DeltaDelete<Key, Data> *node1 = CreateDeltaDelete<Key, Data>();
                node1->key = key;
                node1->origin = node;
                if (!mapping[nextNode].compare_exchange_weak(node, node1)) {
                    free(node1);
                    nextNode = root;
                    deleteKey(key);//TODO without recursion
                    return;
                }
                return;
            }
            default: {
            }
                switch (node->type) {
                    //TODO the ones that are less likely to occur
                }
        }
    }
}

















