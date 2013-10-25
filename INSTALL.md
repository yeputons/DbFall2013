To build this homework, you need the following to be installed in your system:

1. JDK SE >=1.7 (I haven't tested it with Java6)
2. Apache Maven

Warning: BigStressTest requires a several minutes and 3GB of space in your temp folder to run successfully.
If you don't have either of them, just add '@org.junit.Ignore' before public class' definition in BigStressTest.java

If you think you're ready, run the following commands from the root directory of this project:
1. mvn package
   This command should finish with 'BUILD SUCCESS' message. 24 tests should be run.
   If you don't want tests to be run at all, add '-Dmaven.test.skip=true' to the command line
2. cd target
3. java -jar net.yeputons.cscenter.dbfall2013-1.0-SNAPSHOT.jar <arguments>
   Information about arguments is available in README.md.
   If you specify no arguments, error message should appear
