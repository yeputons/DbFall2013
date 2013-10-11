To build this homework, you need the following to be installed in your system:

1. JDK SE >=1.7 (I haven't tested it with Java6)
2. Apache Maven

If you think you're ready, run the following commands from the root directory of this project:
Either method 1:
  1. mvn package
     This command should finish with 'BUILD SUCCESS' message. 18 tests should be run, 0 of them should be skipped
  2. cd target
  3. java -jar net.yeputons.cscenter.dbfall2013-1.0-SNAPSHOT.jar

Or method 2:
  1. mvn compile
     This command should finish with 'BUILD SUCCESS' message.
  2. mvn test
     18 tests should be run, 0 of them should be skipped
  3. mvn exec:java
