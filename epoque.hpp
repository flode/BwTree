#ifndef EPOQUE_HPP
#define EPOQUE_HPP

#include "nodes.hpp"

namespace BwTree {
    template<typename Key, typename Data>
    class Epoque {
        static const std::size_t epoquescount{40000};
        std::atomic<unsigned> epoques[epoquescount];
        std::array<Node<Key, Data> *, 10> deletedNodes[epoquescount];
        std::atomic<std::size_t> deleteNodeNext[epoquescount];
        std::atomic<unsigned> oldestEpoque{0};
        std::atomic<unsigned> newestEpoque{0};

        std::mutex mutex;
        std::size_t openEpoques = 0;
    public:
        Epoque() {
            epoques[newestEpoque].store(0);
            deleteNodeNext[newestEpoque].store(0);
        }

        unsigned enterEpoque();

        void leaveEpoque(unsigned e);

        void markForDeletion(Node<Key, Data> *);
    };

    template<typename Key, typename Data>
    class EnterEpoque {
        Epoque<Key, Data> &epoque;
        unsigned int myEpoque;

    public:
        EnterEpoque(Epoque<Key, Data> &epoque) : epoque(epoque) {
            myEpoque = epoque.enterEpoque();
        }

        virtual ~EnterEpoque() {
            epoque.leaveEpoque(myEpoque);
        }

        Epoque<Key, Data> &getEpoque() const {
            return epoque;
        }
    };


    template<typename Key, typename Data>
    void Epoque<Key, Data>::markForDeletion(Node<Key, Data> *node) {
        std::lock_guard<std::mutex> guard(mutex);
        unsigned e = newestEpoque % epoquescount;
        std::size_t index = deleteNodeNext[e]++;
        if (index == deletedNodes[e].size())
            assert(false); //too many deleted nodes
        deletedNodes[e].at(index) = node;
    }

    template<typename Key, typename Data>
    void Epoque<Key, Data>::leaveEpoque(unsigned e) {
        std::lock_guard<std::mutex> guard(mutex);
        openEpoques--;
        unsigned rest = --epoques[e];
        unsigned oldestEpoque = this->oldestEpoque;
        if (rest == 0) {
            unsigned lastEpoque = oldestEpoque;
            std::vector<Node<Key, Data> *> nodes;
            for (int i = oldestEpoque; i != e; i = (i + 1) % epoquescount) {
                if (epoques[i] == 0) {
                    for (int j = 0; j < deleteNodeNext[i]; ++j) {
                        nodes.push_back(deletedNodes[i].at(j));
                    }
                    lastEpoque = (i + 1) % epoquescount;
                } else {
                    break;
                }
            }
            if (this->oldestEpoque.compare_exchange_weak(oldestEpoque, lastEpoque)) {
                for (auto &node : nodes) {
                    freeNodeRecursively<Key, Data>(node);
                }
            } else {
                std::cerr << "failed";
            }
        }
    }

    template<typename Key, typename Data>
    unsigned Epoque<Key, Data>::enterEpoque() {
        std::lock_guard<std::mutex> guard(mutex);
        openEpoques++;
        if (openEpoques > 100) {
            assert(false);
        }
        if (deleteNodeNext[newestEpoque] == 0) {
            ++epoques[newestEpoque];
            return newestEpoque;
        }
        newestEpoque.store((newestEpoque + 1) % epoquescount); // TODO doesn't change newestEpoque
        if (newestEpoque == oldestEpoque) {
            std::cout << " " << newestEpoque << " " << oldestEpoque;
            //return e;
        }
        assert(newestEpoque != oldestEpoque);// not thread safe
        epoques[newestEpoque].store(1);
        deleteNodeNext[newestEpoque].store(0);
        return newestEpoque;
    }
}

#endif