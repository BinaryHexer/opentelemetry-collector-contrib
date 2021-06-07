FROM golang:1.16 AS build

WORKDIR /src
ADD . /src

RUN make otelcontribcol

FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM alpine:latest

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /src/bin/otelcontribcol_linux_amd64 /otelcontribcol
ENTRYPOINT ["/otelcontribcol"]
EXPOSE 4317 55680 55679
