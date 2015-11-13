#ifndef EPOQUE_HPP
#define EPOQUE_HPP

#include <sys/wait.h>
#include <atomic>
#include "nodes.hpp"
#include "tbb/enumerable_thread_specific.h"
#include "tbb/combinable.h"

namespace BwTree {

    template <typename Key, typename Data>
    struct LabelDelete {
        std::array<Node<Key, Data>*, 16> nodes;
        uint64_t epoche;
        std::size_t nodesCount;
        LabelDelete *next;
    };

    template <typename Key, typename Data>
    class DeletionList {
        LabelDelete<Key, Data> *headDeletionList = nullptr;
        LabelDelete<Key, Data> *freeLabelDeletes = nullptr;
        std::size_t deletitionListCount = 0;

    public:
        std::atomic<uint64_t> localEpoche;

        ~DeletionList();
        LabelDelete<Key, Data> *head();

        void add(Node<Key, Data> *n);

        void remove(LabelDelete<Key, Data> *label, LabelDelete<Key, Data> *prev);

        std::size_t size();

        std::uint64_t deleted = 0;
        std::uint64_t added = 0;
    };

    template <typename Key, typename Data>
    class Epoche;
    template <typename Key, typename Data>
    class EpocheGuard;

    template <typename Key, typename Data>
    class ThreadInfo {
        friend class Epoche<Key, Data>;
        friend class EpocheGuard<Key, Data>;
        Epoche<Key, Data> &epoche;
        DeletionList<Key, Data> &deletionList;

        Epoche<Key, Data> &getEpoche() const;

        DeletionList<Key, Data> &getDeletionList() const;
    public:
        ThreadInfo(Epoche<Key, Data> &epoche);

        ~ThreadInfo();
    };

    template <typename Key, typename Data>
    class Epoche {
        friend class ThreadInfo<Key, Data>;
        std::atomic<uint64_t> currentEpoche{0};

        tbb::enumerable_thread_specific<DeletionList<Key, Data>> deletionLists;

        size_t startGCThreshhold;

        size_t thresholdCounter;

    public:
        Epoche(size_t startGCThreshhold) : startGCThreshhold(startGCThreshhold), thresholdCounter(0) { }

        ~Epoche();

        void enterEpoche(ThreadInfo<Key, Data> &epocheInfo);

        void markNodeForDeletion(Node<Key, Data> *n, ThreadInfo<Key, Data> &epocheInfo);

        void exitEpocheAndCleanup(ThreadInfo<Key, Data> &info);

        void showDeleteRatio();

    };

    template <typename Key, typename Data>
    class EpocheGuard {
        ThreadInfo<Key, Data> &threadEpocheInfo;
    public:

        EpocheGuard(ThreadInfo<Key, Data> &threadEpocheInfo) : threadEpocheInfo(threadEpocheInfo) {
            threadEpocheInfo.getEpoche().enterEpoche(threadEpocheInfo);
        }

        ~EpocheGuard() {
            threadEpocheInfo.getEpoche().exitEpocheAndCleanup(threadEpocheInfo);
        }
    };
}

#endif
