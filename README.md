# Cottontail DB Client Driver

[![Maven Central](https://img.shields.io/maven-central/v/org.vitrivr/cottontaildb-proto.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.vitrivr%22%20AND%20a:%22cottontaildb-proto%22)

These are the [Protocol Buffer v3](https://developers.google.com/protocol-buffers/docs/proto3) and [gRPC](https://grpc.io/) definitions (= client driver) for Cottontail DB. They can be used to build a client that uses Cottontail DB as a storage layer / database.

For examples as to how to use the driver, we refer to the [Cottontail DB Examples Project](https://github.com/vitrivr/cottontaildb-examples).

## How To Use

The easiest way to use this client driver is to include the artifact using Maven (or your build automation tool of choice):

```
<dependency>
  <groupId>org.vitrivr</groupId>
  <artifactId>cottontaildb-proto</artifactId>
  <version>VERSION</version>
</dependency>
```

Where `VERSION` can be found from the badge above

Of course, you can also checkout the repository and build the files using `protoc` or simply `./gradlew clean generateProto`

## Versions
Please mind that different versions of Cottontail DB might require different versions of the driver.
