#!/bin/bash

thrift --gen java Address.thrift
thrift --gen java NodeService.thrift
thrift --gen java ServerService.thrift
rm Address.java
rm NodeService.java
rm ServerService.java
cp gen-java/Address.java ./
cp gen-java/NodeService.java ./
cp gen-java/ServerService.java ./
rm -r gen-java

javac -cp ".:/usr/local/Thrift/*" *.java -d .
