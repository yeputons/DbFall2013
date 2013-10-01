== Building

To build this homework, you need the following to be installed in your system:

1. JDK SE >=1.7 (I haven't tested it with Java6)
2. Apache Maven

If you think you're ready, run the following commands from the root directory of this probject:
1. mvn package
   This command should finish with 'BUILD SUCCESS' message. If not, something went wrong
2. cd target
3. java -jar net.yeputons.cscenter.dbfall2013-1.0-SNAPSHOT.jar
