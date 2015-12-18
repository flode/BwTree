#include <assert.h>
#include <iostream>
#include "epoque.hpp"

namespace BwTree {

    template<typename Key, typename Data>
    DeletionList<Key, Data>::~DeletionList() {
        assert(deletitionListCount == 0 && headDeletionList == nullptr);
        LabelDelete<Key, Data> *cur = nullptr, *next = freeLabelDeletes;
        while (next != nullptr) {
            cur = next;
            next = cur->next;
            delete cur;
        }
        freeLabelDeletes = nullptr;
    }

    template<typename Key, typename Data>
    std::size_t DeletionList<Key, Data>::size() {
        return deletitionListCount;
    }

    template<typename Key, typename Data>
    void DeletionList<Key, Data>::remove(LabelDelete<Key, Data> *label, LabelDelete<Key, Data> *prev) {
        if (prev == nullptr) {
            headDeletionList = label->next;
        } else {
            prev->next = label->next;
        }
        deletitionListCount -= label->nodesCount;

        label->next = freeLabelDeletes;
        freeLabelDeletes = label;
        // TODO max of freelabeldeletes
        deleted += label->nodesCount;
    }

    template<typename Key, typename Data>
    void DeletionList<Key, Data>::add(Node<Key, Data> *n) {
        deletitionListCount++;
        LabelDelete<Key, Data> *label;
        if (headDeletionList != nullptr && headDeletionList->nodesCount < headDeletionList->nodes.size()) {
            label = headDeletionList;
        } else {
            if (freeLabelDeletes != nullptr) {
                label = freeLabelDeletes;
                freeLabelDeletes = freeLabelDeletes->next;
            } else {
                label = new LabelDelete<Key, Data>();
            }
            label->nodesCount = 0;
            label->next = headDeletionList;
            headDeletionList = label;
        }
        label->nodes[label->nodesCount] = n;
        label->nodesCount++;
        label->epoche = localEpoche;

        added++;
    }

    template<typename Key, typename Data>
    LabelDelete<Key, Data> *DeletionList<Key, Data>::head() {
        return headDeletionList;
    }

    template<typename Key, typename Data>
    void Epoche<Key, Data>::enterEpoche(ThreadInfo<Key, Data> &epocheInfo) {
        unsigned long curEpoche = currentEpoche.load();
        if (curEpoche != epocheInfo.getDeletionList().localEpoche) {
            epocheInfo.getDeletionList().localEpoche.store(curEpoche);
        }
    }

    template<typename Key, typename Data>
    void Epoche<Key, Data>::markNodeForDeletion(Node<Key, Data> *n, ThreadInfo<Key, Data> &epocheInfo) {
        epocheInfo.getDeletionList().add(n);
        epocheInfo.getDeletionList().thresholdCounter++;
    }

    template<typename Key, typename Data>
    void Epoche<Key, Data>::exitEpocheAndCleanup(ThreadInfo<Key, Data> &epocheInfo) {
        auto &deletionList = epocheInfo.getDeletionList();
        if ((deletionList.thresholdCounter & (64 - 1)) == 0) {
            currentEpoche.fetch_add(1);
        }
        if (deletionList.thresholdCounter > startGCThreshhold) {
            if (deletionList.size() == 0) {
                deletionList.thresholdCounter = 1;
                return;
            }
            deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());

            uint64_t oldestEpoche = std::numeric_limits<uint64_t>::max();
            for (auto &epoche : deletionLists) {
                auto e = epoche.localEpoche.load();
                if (e < oldestEpoche) {
                    oldestEpoche = e;
                }
            }

            LabelDelete<Key, Data> *cur = deletionList.head(), *next, *prev = nullptr;
            while (cur != nullptr) {
                next = cur->next;

                if (cur->epoche < oldestEpoche) {
                    for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                        freeNodeRecursively(cur->nodes[i]);
                    }
                    deletionList.remove(cur, prev);
                } else {
                    prev = cur;
                }
                cur = next;
            }
            deletionList.thresholdCounter = 1;
        }
    }

    template<typename Key, typename Data>
    Epoche<Key, Data>::~Epoche() {
        uint64_t oldestEpoche = std::numeric_limits<uint64_t>::max();
        for (auto &epoche : deletionLists) {
            auto e = epoche.localEpoche.load();
            if (e < oldestEpoche) {
                oldestEpoche = e;
            }
        }
        for (auto &d : deletionLists) {
            LabelDelete<Key, Data> *cur = d.head(), *next, *prev = nullptr;
            while (cur != nullptr) {
                next = cur->next;

                assert(cur->epoche < oldestEpoche);
                for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                    freeNodeRecursively(cur->nodes[i]);
                }
                d.remove(cur, prev);
                cur = next;
            }
        }
    }

    template<typename Key, typename Data>
    void Epoche<Key, Data>::showDeleteRatio() {
        for (auto &d : deletionLists) {
            std::cout << "deleted " << d.deleted << " of " << d.added << std::endl;
        }
    }

    template<typename Key, typename Data>
    ThreadInfo<Key, Data>::ThreadInfo(Epoche<Key, Data> &epoche)
            : epoche(epoche), deletionList(epoche.deletionLists.local()) { }

    template<typename Key, typename Data>
    DeletionList<Key, Data> &ThreadInfo<Key, Data>::getDeletionList() const {
        return deletionList;
    }

    template<typename Key, typename Data>
    Epoche<Key, Data> &ThreadInfo<Key, Data>::getEpoche() const {
        return epoche;
    }

    template<typename Key, typename Data>
    ThreadInfo<Key, Data>::~ThreadInfo() {
        deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());
    }
}