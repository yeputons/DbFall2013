DbFall2013
==========

Computer Science Center, fall 2013, course 'Databases', homework.

You can run this app in batch mode by passing '--batch' option. This option
disables greeting and command prompts, which may make batch use possible

== Installation

See INSTALL.md

== Usage

By default, HashTrieStorage is enabled. It should require O(1) memory regardless of DB size.
However, it does not support DBs files larger than 2GB.

If you are not satisfied somewhy, you can enable LogFileEngine or InMemoryENgine instead. To do this,
edit ConsoleClient.java and just re-evaluate all steps from the 'installation' section.

No engine supports null keys of values. All of them implements Map<ByteBuffer, ByteBuffer>

'clear' operation is implemented by removing storage file. All stored numbers
are 32-bit signed integers, high byte first.

== InMemoryEngine

Uses HashMap<>, does not store anything to the disk.

== LogFileEngine

Extends InMemoryEngine. Writes every operation to the disk.

Storage format:

First four bytes: amount of logged operations (int32)
Next bytes are records. If anything appears after the records, the storage is truncated during initial load.

Each record is a sequence of bytes describing some action:

First 4 bytes: length of key (int32).
Next bytes: the key itself
Next 4 bytes: length of value (int32). It can be -1 too, which means 'remove this key from DB'.
Next bytes: the value itself
