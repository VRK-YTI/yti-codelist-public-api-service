# YTI CodeList - Public API Service microservice

This application is part of the [Joint metadata and information management programme](https://wiki.julkict.fi/julkict/yti).

## Description

This is the implementation of the Public API Service microservice for the YTI CodeList with:

* [Spring boot] For getting things up and running
* Embedded [Jetty] to serve
* [Jersey 2] for JAX-RS

### Example queries:

Do a HTTP GET to public resource:
`http://localhost:9601/api/v1/hello`

## Interface Documentation

When the microservice is running, you can get the Swagger REST API documentation from:
- [http://localhost:9601/api/swagger.json](http://localhost:9601/codelist-api/api/swagger.json)
- [http://localhost:9601/swagger/index.html](http://localhost:9601/codelist-api/swagger/index.html)

## Prerequisities

### Building
- Java 8+
- Maven 3.3+
- Docker

## Running

- [yti-codelist-config](https://github.com/vrk-yti/yti-codelist-config) - Default configuration for development use

## Starting service on local development environment

### Running inside IDE

Add the following Run configurations options:

- Program arguments: `--spring.profiles.active=local --spring.config.location=../yti-codelist-config/application.yml,../yti-codelist-config/yti-codelist-public-api-service.yml`
- Workdir: `$MODULE_DIR$`

Add folder for yti project, application writes modified files there:

```bash
$ mkdir /data/yti
```


### Building the Docker Image

```bash
$ mvn clean package docker:build
```

### Running the Docker Image

```bash
$ docker run --rm -p 9601:9601 -p 19601:19601 -v /path/to/yti-codelist-config:/config --name=yti-codelist-public-api-service yti-codelist-public-api-service -a --spring.config.location=/config/application.yml,/config/yti-codelist-public-api-service.yml
```

.. or in [yti-codelist-compose](https://github.com/vrk-yti/yti-codelist-compose/) run

```bash
$ docker-compose up yti-codelist-public-api-service
```

[Spring boot]:http://projects.spring.io/spring-boot/
[Jetty]:http://www.eclipse.org/jetty/
[Jersey 2]:https://jersey.java.net
