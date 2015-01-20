#ifndef EPOQUE_HPP
#define EPOQUE_HPP

#include <sys/wait.h>
#include "nodes.hpp"

namespace BwTree {
    template<typename Key, typename Data>
    class Epoque {
        static constexpr std::size_t epoquescount{40000};
        std::atomic<std::size_t> epoques[epoquescount];
        std::vector<std::array<Node<Key, Data> *, 11>> deletedNodes{epoquescount};
        std::atomic<std::size_t> deleteNodeNext[epoquescount];
        std::atomic<std::size_t> oldestEpoque{0};
        std::atomic<std::size_t> newestEpoque{0};

        std::mutex mutex;
        std::atomic<std::size_t> openEpoques{0};
    public:
        Epoque() {
            epoques[newestEpoque].store(0);
            deleteNodeNext[newestEpoque].store(0);
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
            myEpoque = epoque.enterEpoque();
        }

        ~EnterEpoque() {
            epoque.leaveEpoque(myEpoque);
        }

        Epoque<Key, Data> &getEpoque() const {
            return epoque;
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

    template<typename Key, typename Data>
    size_t Epoque<Key, Data>::enterEpoque() {
        std::lock_guard<std::mutex> guard(mutex);
        openEpoques++;
        assert(openEpoques < 300);
        if (deleteNodeNext[newestEpoque] == 0) {
            ++epoques[newestEpoque];
            return newestEpoque;
        }
        newestEpoque.store((newestEpoque + 1) % epoquescount); // TODO doesn't change newestEpoque
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