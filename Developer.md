## Requirements

* Windows, Mac, or Linux
* Java 8 Update 151 or higher, 64-bit. OpenJDK preferred
* Gradle 3.3 and Maven 3.3.9+ (for building)
* IntelliJ IDEA 2018.1 or higher

### **Building Skydrill**

Skydrill is a standard Gradle project. Simply run the following command from the project root directory:

    gradle clean makeTarget

On the first build, Gradle will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Skydrill can be built on Windows, Linux, as well as OS X. To build on Windows, you can use the preassembled set of build tools in the SkyDrill.BuildTools project. Simply clone the repository [BuildTools](https://https://github.com/davidlao2k/win-build.git), and execute setenv.cmd in a cmd window to prepare for build environment.

You will find the deployable build in the target directory. 

### **Debugging**

Skydrill builds on Apache Presto in the form of class extensions, so to debug Skydrill you will need to have the corresponding version of Presto source available. To enable source level debugging, simply include the following JVM option:

    -agentlib:jdwp=transport=dt_socket,server=y,address=%DEBUG_PORT%,suspend=n

and use IntelliJ to attach to the local process. On linux/Mac, you can add this option to the jvm.config file in the etc directory. On Windows, the option is enabled automatically when you set value to the environment variable `DEBUG_PORT`

### **Running Skydrill Server**

On Windows, use the skydrill.cmd command tool to launch:

    bin\skydrill.cmd server

On Linux/Mac, update `bin/launcher.properties` with the skydrill main class

    main-class=io.panyu.skydrill.server.SkydrillServer
    process-name=skydrill-server

and use the standard Presto launcher to launch:

    bin/launcher run

### **Running the CLI**

Start the CLI to connect to the server and run SQL queries:

    java -jar bin/cli/presto-cli-*-executable.jar

or on Windows:

    bin\skydrill.cmd cli

Run a query to see the nodes in the cluster:

    SELECT * FROM system.runtime.nodes;

Run a query to show the tables in the tpch database

    SHOW TABLES FROM tpch.tiny;
