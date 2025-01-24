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
RUN apt install -y ca-certificates curl xz-utils
RUN curl https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz --output /tmp/ffmpeg.tar.xz
RUN echo "7fa72b652e19bf84c9461e332ea1cdf3 /tmp/ffmpeg.tar.xz" | md5sum -c -
RUN mkdir /tmp/ffmpeg
RUN tar xvf /tmp/ffmpeg.tar.xz -C /tmp/ffmpeg
RUN mv /tmp/ffmpeg/ffmpeg-7.0.2-amd64-static/ffmpeg /usr/bin/ffmpeg
RUN which ffmpeg
RUN update-ca-certificates -f
CMD ["/douga"]
