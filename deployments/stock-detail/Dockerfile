FROM golang:1.21-alpine

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /app/cmd/stock-detail
RUN go build -o /bin/stock-detail main.go

EXPOSE 8080
CMD ["/bin/stock-detail"]