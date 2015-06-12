# BwTree
This Implementation of the BwTree is available under the GNU GENERAL PUBLIC LICENSE

It was developed at the TU Munich as a student project at the Chair for database systems.

The BwTree is a lock free in-memory B+ Tree.

The principles have been described by Microsoft Research:

The Bw-Tree: A B-tree for New Hardware Platforms: http://research.microsoft.com/pubs/178758/bw-tree-icde2013-final.pdf


## Build instructions
    mkdir build
    cd build/
    cmake ..
    make


## Execution instructions
    ./BwTree

By default different artificial test cases are emulated for performance measurements.
The test cases can be changed in the main.cpp file.

By executing with tcmalloc performance can be improved.

## Restrictions of this implementation:
- Merging underful pages is not implemented.
- The epoque system uses locks to keep track of pages that can be deleted
- The mapping table cannot grow dynamically


