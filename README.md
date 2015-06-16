# BwTree

This demo implementation of a BwTree was developed as a student project at the Chair for database systems at the TU Munich.

The BwTree is a lock free in-memory B+ Tree, the principles have been described by Microsoft Research:

"The Bw-Tree: A B-tree for New Hardware Platforms": http://research.microsoft.com/pubs/178758/bw-tree-icde2013-final.pdf


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
- Necessary operations like consolidate or split are not executed asynchronously


## License
Copyright 2014-2015 Florian Scheibner

This Implementation of the BwTree is available under the GNU GENERAL PUBLIC LICENSE v3
