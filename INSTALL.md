To build this homework, you need the following to be installed in your system:

1. JDK SE >=1.7 (I haven't tested it with Java6)
2. Apache Maven

Warning: BigStressTest requires a several minutes and 3GB of space in your temp folder to run successfully.
If you don't have either of them, just add '@org.junit.Ignore' before public class' definition in BigStressTest.java

If you think you're ready, run the following commands from the root directory of this project:
Either method 1:
  1. mvn package
     This command should finish with 'BUILD SUCCESS' message. 22 tests should be run, 0 of them should be skipped
  2. cd target
  3. java -jar net.yeputons.cscenter.dbfall2013-1.0-SNAPSHOT.jar

Or method 2:
  1. mvn compile
     This command should finish with 'BUILD SUCCESS' message.
  2. mvn test
     22 tests should be run, 0 of them should be skipped
  3. mvn exec:java
