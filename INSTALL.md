== Building

To build this homework, you need the following to be installed in your system:

1. JDK SE >=1.7 (I haven't tested it with Java6)
2. Apache Maven

If you think you're ready, run the following commands from the root directory of this probject:
1. mvn package
   This command should finish with 'BUILD SUCCESS' message. 17 tests should be run, 1 of them should be skipped
2. cd target
3. java -jar net.yeputons.cscenter.dbfall2013-1.0-SNAPSHOT.jar

== Usage

By default, HashTrieStorage is enabled. It's slow as hell, but should require O(1) memory regardless of DB size.

If you are not satisfied with its speed, you can enable LogFileEngine instead. To do this, comment ConsoleClient.java:58 and
uncomment the next line. After that just re-evaluate all steps from the 'building' section.
