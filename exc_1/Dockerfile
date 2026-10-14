FROM golang:1.22-alpine
WORKDIR /app
COPY go.mod ./
COPY *.go ./
RUN go build -o bigdata-ex1
EXPOSE 8080
CMD ["./bigdata-ex1"]
