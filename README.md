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
- The mapping table cannot grow dynamically
- Necessary operations like consolidate or split are not executed asynchronously

## Troubleshooting
- Error compiling: "error: invalid value 'c++14' in '-std=c++14'"

You need a compiler that supports C++14 in order to compile the project.

## License
Copyright 2014-2015 Florian Scheibner

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
