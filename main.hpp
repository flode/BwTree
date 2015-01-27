#define BLOCK(){std::string c;std::cout << "...";std::cin >> c;}

enum class BwTreeCommandType : std::int8_t {
    insert,
    search
};

template<typename Key, typename Data>
struct BwTreeCommand {
    const BwTreeCommandType type;
    const Key key;
    const Data *data;

    BwTreeCommand(BwTreeCommandType const &type, Key const key, Data const *data) : type(type), key(key), data(data) {
    }
};

template<typename Key>
std::chrono::milliseconds createBwTreeCommands(const std::size_t numberOfThreads, const std::vector<Key> &values, const std::vector<Key> &initial_values, const std::size_t operations, const unsigned percentRead, BwTree::Tree<Key, Key> &tree, bool block);

template<typename Key>
void executeBwTreeCommands(const std::vector<std::vector<BwTreeCommand<Key, Key>>> &commands, BwTree::Tree<Key, Key> &tree);
