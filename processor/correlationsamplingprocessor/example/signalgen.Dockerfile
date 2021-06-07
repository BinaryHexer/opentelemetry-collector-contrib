FROM golang:1.16 AS build

WORKDIR /src
ADD . /src
WORKDIR /src/signalgen

ENV GOOS=linux
ENV GOARCH=amd64
ENV GO111MODULE=on
ENV CGO_ENABLED=0

RUN go build -trimpath -o /src/bin/signalgen_${GOOS}_${GOARCH} .

FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM alpine:latest

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /src/bin/signalgen_linux_amd64 /signalgen
ENTRYPOINT ["/signalgen"]
