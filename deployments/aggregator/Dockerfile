FROM golang:1.21-alpine
RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
WORKDIR /app/cmd/aggregator
RUN go build -o /bin/aggregator main.go

EXPOSE 0
CMD ["/bin/aggregator"]