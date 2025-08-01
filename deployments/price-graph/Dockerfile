FROM golang:1.21-alpine

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go mod tidy

WORKDIR /app/cmd/price-graph
RUN go build -o /bin/price-graph main.go

EXPOSE 8081
CMD ["/bin/price-graph"]