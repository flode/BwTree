#ifndef EPOQUE_HPP
#define EPOQUE_HPP

#include <sys/wait.h>
#include "nodes.hpp"

namespace BwTree {

    static thread_local std::size_t lastEpoque = std::numeric_limits<std::size_t>::max();
    static thread_local std::size_t lastEpoqueCount{0};


    template<typename Key, typename Data>
    class Epoque {
        static constexpr std::size_t epoquescount{40000};
        std::atomic<std::size_t> epoques[epoquescount];
        std::vector<std::array<Node<Key, Data> *, 40>> deletedNodes{epoquescount};
        std::atomic<std::size_t> deleteNodeNext[epoquescount];
        std::atomic<std::size_t> oldestEpoque{0};
        std::atomic<std::size_t> newestEpoque{0};

        std::mutex mutex;
        std::atomic<std::size_t> openEpoques{0};
    public:
        Epoque() {
            epoques[newestEpoque].store(0);
            deleteNodeNext[newestEpoque].store(0);

            lastEpoque = std::numeric_limits<std::size_t>::max();
            lastEpoqueCount = 0;
        }

        ~Epoque() {
            for (std::size_t i = oldestEpoque; i != newestEpoque; i = (i + 1) % epoquescount) {
                for (std::size_t j = 0, end = deleteNodeNext[i]; j < end; ++j) {
                    freeNodeRecursively<Key, Data>(deletedNodes[i].at(j));
                }
                deleteNodeNext[i].store(0);
            }
        }

        size_t enterEpoque();

        void leaveEpoque(size_t e);

        void markForDeletion(Node<Key, Data> *);
    };

    template<typename Key, typename Data>
    class EnterEpoque {
        Epoque<Key, Data> &epoque;
        std::size_t myEpoque;
    public:
        EnterEpoque(Epoque<Key, Data> &epoque) : epoque(epoque) {
            if (lastEpoque != std::numeric_limits<std::size_t>::max()) {
                myEpoque = lastEpoque;
            } else {
                myEpoque = epoque.enterEpoque();
                lastEpoque = myEpoque;
            }
        }

        ~EnterEpoque() {
            if (lastEpoqueCount > 10) {
                epoque.leaveEpoque(myEpoque);
                lastEpoque = std::numeric_limits<std::size_t>::max();
                lastEpoqueCount = 0;
            } else {
                lastEpoqueCount++;
            }
        }

        static void threadFinishedWithTree(Epoque<Key, Data> &epoque) {
            if (lastEpoque != std::numeric_limits<std::size_t>::max()) {
                epoque.leaveEpoque(lastEpoque);
                lastEpoque = std::numeric_limits<std::size_t>::max();
            }
        }
    };


    template<typename Key, typename Data>
    void Epoque<Key, Data>::markForDeletion(Node<Key, Data> *node) {
        std::lock_guard<std::mutex> guard(mutex);
        std::size_t e = newestEpoque % epoquescount;
        std::size_t index = deleteNodeNext[e]++;
        assert(index < deletedNodes[e].size());//too many deleted nodes
        deletedNodes[e].at(index) = node;
    }

    template<typename Key, typename Data>
    void Epoque<Key, Data>::leaveEpoque(const size_t e) {
        openEpoques--;
        if (--epoques[e] > 0 || !mutex.try_lock()) {
            return;
        }
        static thread_local std::vector<Node<Key, Data> *> nodes;
        nodes.clear();
        std::size_t oldestEpoque = this->oldestEpoque;
        std::size_t i;
        for (i = oldestEpoque; i != e; i = (i + 1) % epoquescount) {
            if (epoques[i] == 0) {
                for (std::size_t j = 0, end = deleteNodeNext[i]; j < end; ++j) {
                    nodes.push_back(deletedNodes[i].at(j));
                }
                deleteNodeNext[i].store(0);
            } else {
                break;
            }
        }
        std::size_t lastEpoque = i;
        if (oldestEpoque == lastEpoque) {
            mutex.unlock();
            return;
        }
        bool exchangeSuccessful = this->oldestEpoque.compare_exchange_weak(oldestEpoque, lastEpoque);

        if (exchangeSuccessful) {
            for (auto &node : nodes) {
                freeNodeRecursively<Key, Data>(node);
            }
        } else {
            std::cerr << "failed";
        }
        mutex.unlock();
    }

//     template<typename Key, typename Data>
// void EpoqueManager<Key, Data>::leaveEpoque(size_t e) {
//     openEpoques--;
//     if (--epoques[e] > 0 || !mutex.try_lock()) {
//         return;
//     }
//     std::array<Node<Key, Data> *, 20> nodes;
//     std::size_t nextNodeIndex = 0;
//     std::size_t oldestEpoque = this->oldestEpoque;
//
//     std::size_t i;
//     for (i = oldestEpoque; i != e; i = (i + 1) % epoquescount) {
//         if (epoques[i] == 0) {
//             //unsigned long deletedNodeIndex = deleteNodeNext[i].load() - 1;
//             bool nodesFull = false;
//             for (unsigned long j = 0; j < deleteNodeNext[i].load(); ++j) {
//                 nodes[nextNodeIndex] = deletedNodes[i].at(deleteNodeNext[i].load() - j - 1);
//                 nextNodeIndex++;
//                 if (nextNodeIndex == nodes.size()) {
//                     nodesFull = true;
//                     if (deleteNodeNext[i].load() - j - 1 != 0) {
//                         std::cerr << "not all freed" << std::endl;
//                         deleteNodeNext[i].store(deleteNodeNext[i].load() - j - 1);
//                         break;
//                     } else {
//                         i = (i + 1) % epoquescount;
//                         break;
//                     }
//                 }
//             }
//             if (nodesFull) {
//                 break;
//             }
//         } else {
//             break;
//         }
//     }
//     std::size_t lastEpoque = i;
//     if (oldestEpoque == lastEpoque) {
//         mutex.unlock();
//         return;
//     }
//     bool exchangeSuccessful = this->oldestEpoque.compare_exchange_weak(oldestEpoque, lastEpoque);
//
//     if (exchangeSuccessful) {
//         for (std::size_t i = 0; i < nextNodeIndex; ++i) {
//             freeNodeRecursively<Key, Data>(nodes[i]);
//         }
//     } else {
//         std::cerr << "failed";
//     }
//     mutex.unlock();
// }

    template<typename Key, typename Data>
    size_t Epoque<Key, Data>::enterEpoque() {
        std::lock_guard<std::mutex> guard(mutex);
        openEpoques++;
        assert(openEpoques < 300);
        if (deleteNodeNext[newestEpoque] == 0) {
            ++epoques[newestEpoque];
            return newestEpoque;
        }
        newestEpoque.store((newestEpoque + 1) % epoquescount);
        if (newestEpoque == oldestEpoque) {
            std::cout << " " << newestEpoque << " " << oldestEpoque;
        }
        assert(newestEpoque != oldestEpoque);// not thread safe
        epoques[newestEpoque].store(1);
        deleteNodeNext[newestEpoque].store(0);
        return newestEpoque;
    }
}

#endif
