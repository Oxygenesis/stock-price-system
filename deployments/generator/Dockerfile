FROM golang:1.21-alpine

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /app/cmd/generator
RUN go build -o /bin/generator main.go

EXPOSE 8000
CMD ["/bin/generator"]