FROM golang:1.23-bookworm
ENV CGO_ENABLED=1
ADD go.mod /src/go.mod
ADD go.sum /src/go.sum

WORKDIR /src
RUN go mod download -x

ADD . /src
RUN go build -o douga

FROM debian:bookworm
COPY --from=0 /src/douga /douga
RUN apt update
RUN apt install -y ca-certificates
RUN update-ca-certificates -f
CMD ["/douga"]
