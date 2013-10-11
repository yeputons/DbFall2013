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

By default, HashTrieStorage is enabled. It should require O(1) memory regardless of DB size.
However, it does not support DBs files larger than 2GB.

If you are not satisfied somewhy, you can enable LogFileEngine or InMemoryENgine instead. To do this,
edit ConsoleClient.java and just re-evaluate all steps from the 'installation' section.

No engine supports null keys of values. All of them implements Map<ByteBuffer, ByteBuffer>

'clear' operation is implemented by removing storage file. All stored numbers
are 32-bit signed integers, high byte first.

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
