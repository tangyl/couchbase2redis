FROM golang

ADD ./ /go/src/couchbase2redis/

RUN cd /go/src/couchbase2redis/ && go get && go build && go install

WORKDIR /go/