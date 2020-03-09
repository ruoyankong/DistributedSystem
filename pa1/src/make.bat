thrift-0.9.3.exe --gen java Address.thrift
thrift-0.9.3.exe --gen java NodeService.thrift
thrift-0.9.3.exe --gen java ServerService.thrift
del Address.java
del NodeService.java
del ServerService.java
copy gen-java\Address.java .\
copy gen-java\NodeService.java .\
copy gen-java\ServerService.java .\
rd gen-java /s /q

