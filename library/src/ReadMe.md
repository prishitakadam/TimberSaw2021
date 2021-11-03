## Steps for creating shared library

### Creating the class and header file
```bash
javac -h . TimberSawJNI.java
```

### Creating the object file
```bash 
g++ --std=c++11 -fpic -I. -I/usr/lib/jvm/java-1.11.0-openjdk-amd64/include -I/usr/lib/jvm/java-1.11.0-openjdk-amd64/include/linux -c -I /scratch1/pkadam/TimberSaw2021/include/ -I/scratch1/pkadam/TimberSaw2021/build/ -I/scratch1/pkadam/TimberSaw2021 TimberSawJNI.cpp -o TimberSawJNI.o
```

### Creating the shared library
```bash
g++ --std=c++11 -fpic -I. -I/usr/lib/jvm/java-1.11.0-openjdk-amd64/include -I/usr/lib/jvm/java-1.11.0-openjdk-amd64/include/linux -c -I /scratch1/pkadam/TimberSaw2021/include/ -I/scratch1/pkadam/TimberSaw2021/build/ 
```
