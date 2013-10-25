DbFall2013
==========

Computer Science Center, fall 2013, course 'Databases', homework.

You can run this app in batch mode by passing '--batch' option. This option
disables greeting and command prompts, which may make batch use possible

Installation
============

See INSTALL.md

Usage
=====

Three options of running JAR is available:

1. 'client'. Simple console client. By default, it accesses distributed database based on configuration specified in
   'sharding.yaml'.
2. 'node'. Starts one sharding node. Requires path to storage file, bind IP (for example, 127.0.0.1) and port as its parameters
3. 'all_nodes'. Starts locally all nodes for configuration specified in sharding.yaml (in different threads of one process).

If you are not satisfied somewhy, you can enable HashTrieStorage, LogFileEngine or InMemoryEngine in client instead. To do this,
edit ConsoleClient.java and just re-evaluate all steps from the 'installation' section.

For example, type these commands to test sharding:

~~~~
java -jar target/net.yeputons.cscenter.dbfall2013-1.0-SNAPSHOT.jar all_nodes
java -jar target/net.yeputons.cscenter.dbfall2013-1.0-SNAPSHOT.jar client
~~~~

## Sharding configuration

## Sharding protocol
Protocol is pretty straightforward - client initiates connection to a shard and sends commands, while
shards sends replies.

Command is a three-byte string (`clr`, `siz`, `del`, `put`, `get`). Then its arguments follow,
with arrays represented as 32-bit integer length (in bytes, high byte first) and its content then.

Each command returns two-byte string (`ok` or `no`) representing result of execution. `no` is followed
by an array (encoded as above), specifying human-readable error message (in ASCII).

1. `clr` - no arguments
2. `siz` - no arguments
3. `del` - key follows
4. `put` - key and value follows
5. `get` - key follows. Returns either array with `length == -1` and no elements, if no such element is presented
   or element's value as array otherwise.
6. `hi!` - just replies 'ok'. Useful for connection testing
7. `key` - answers with amount of actual keys (32-bit), followed by them in standard format (length + data bytes)
8. `its` - answers with amount of actual keys (32-bit), followed by items: key first (length + data), value follows in the same format.
9. `pak` - starts compacting process. Do not return until the process finishes.
10. `dwn` - terminates the node gracefully.

## Storage engines

No engine supports null keys of values. All of them implements Map<ByteBuffer, ByteBuffer>

All stored numbers are 32-bit signed integers, high byte first.

### Router

It's a fake engine, which provides access to sharding cluster. Please note, that its 'remove' and 'put' return null instead of
old value of the corresponding key.

### InMemoryEngine

Uses HashMap<>, does not store anything to the disk.

### LogFileEngine

Extends InMemoryEngine. Writes every operation to the disk.

Storage format:

First four bytes: amount of logged operations (int32)
Next bytes are records. If anything appears after the records, the storage is truncated during initial load.

Each record is a sequence of bytes describing some action:

First 4 bytes: length of key (int32).
Next bytes: the key itself
Next 4 bytes: length of value (int32). It can be -1 too, which means 'remove this key from DB'.
Next bytes: the value itself

### HashTrieEngine

This engine stores trie (prefix tree) of keys' SHA1 hashes. Each node may have up to 256 children
and maximal depth of such trie is at most 160/8 = 20 edges.

There are two types of nodes: inner and leaf. Inner node contains links to its children, while leaf
node contains one pair: (key, value). Value can be null which means 'this pair was deleted'.

If inner node contains only one leaf in its subtree, all unnecessary inner nodes are not stored.
Therefore, depth of particular leafs can be less than 20 and can even change during DB work.

Storage file is preallocated to be mmap() usable. If it becomes too small to hold
all the date, its size is increased twice.

Storage format:
First 4 bytes - signature. {'Y', 'D', 'B', 2} as for now
Next 8 bytes - current length of 'busy' part of the file (which was used)

After that descriptions of nodes follow. Root node is always presented at offset 12 and it's always inner.

First byte of each node's specification is its type:
1. Inner node has type=1. After that 256 int64 values follow - offsets of children of this inner node.
   Unexisting child is represented by offset zero, which is invalid offset.
2. Leaf node has type=2. Then its value's length follows (int32, in bytes, or -1 for null). Then the value
   goes itself, then key's length (int32), then the key.

Content of the unused part of storage is undefined and is not used.
