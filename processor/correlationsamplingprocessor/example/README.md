# Correlation sampling demo

Supported pipeline types: traces, logs

## How to run

1. From the root of the project, run:
```shell
docker build -t csp . -f processor/correlationsamplingprocessor/example/otelcontrib.Dockerfile
docker build -t signalgen . -f processor/correlationsamplingprocessor/example/signalgen.Dockerfile
```

2. Then from this directory (processor/correlationsamplingprocessor/example), run:
```shell
docker-compose up
```

## How does it work

- The `load-generator` container is used as a trace and log generator.
- The `otel-collector` container are used to represent otel-collector that runs correlated sampling processor.