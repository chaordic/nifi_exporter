# nifi_exporter

Apache NIFI exporter for collect data from API.

## Collectors


[nifi/collectors/processgroups.go](https://github.com/chaordic/nifi_exporter/blob/master/nifi/collectors/processgroups.go)

### Enabled by default

> TODO

### Disabled by default

> TODO

## Setup

Copy sample configuration and adapt it to yours.

`cp sample-config.yml my-nifi.yml`


## Building and running

Prerequisites:

* [Go compiler](https://golang.org/dl/)

Building:

    GOPATH=~/.go go get github.com/prometheus/node_exporter
    cd ${GOPATH-$HOME/go}/src/github.com/msiedlarek/nifi_exporter
    GOPATH=~/.go go build
    ./node_exporter <config_file>

## Running tests

    make test


