FROM golang:1.9.1

WORKDIR /go/src/github.com/awesomeProject/homie-search/app

COPY app .

RUN go install -v ./...

EXPOSE 8080

CMD ["app"]
