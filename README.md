# ConsistentHashing
Consistent Hashing Algorithm implemented in Java

This is the readme for the Bootstrap.java class, which implements a bootstrap server for a distributed key-value store using a consistent hashing ring.

Functionality:

    Manages the ring of NameServers by keeping track of their IDs, IPs, and ports.
    Allows NameServers to join and leave the ring.
    Handles data transfer between NameServers during join/leave operations.
    Provides a command-line interface for basic key-value store operations (lookup, insert, delete).

Classes Used:

    Bootstrap: The main class responsible for bootstrapping the network and handling NameServer interactions.
    NSOperations: Encapsulates key-value store operations like lookup, insert, and delete.
    NSMeta: Stores metadata about a NameServer, including its ID, IP, ports, and predecessor/successor information.
    BootstrapUI: Runs in a separate thread and provides a command-line interface for user interaction.

How to Run:

    Compile the code:

javac Bootstrap.java NSOperations.java NSMeta.java BootstrapUI.java

    Run the Bootstrap server:

java Bootstrap <config_file>

    <config_file>: Path to a text file containing the server configuration.
        The first line should be the server's ID.
        The second line should be the server's port number.
        Optional lines can follow, each containing a key-value pair to be pre-populated in the data store.

Example Configuration File (config.txt):
```
100
4578
1 A
101 B
```
Command-Line Interface:

The Bootstrap server provides a command-line interface for basic key-value store operations:

    lookup <key>: Looks up the value for a given key.
    insert <key> <value>: Inserts a key-value pair into the network.
    delete <key>: Deletes a key-value pair from the network.

Notes:

    This code assumes a simple key-value store where keys are integers.
    Error handling is included for basic scenarios like invalid commands or key ranges.
    The implementation focuses on core functionalities and may not include advanced features like fault tolerance or replication.

Further Exploration:

    You can modify the code to support different key types (strings, etc.).
    Implement additional key-value store operations like update.
    Enhance error handling and logging for more robust operation.
    Explore techniques for fault tolerance and data replication.
