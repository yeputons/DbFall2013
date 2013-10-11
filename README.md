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
