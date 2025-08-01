FROM golang:1.21-alpine
RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /app/cmd/portfolio
RUN go build -o /bin/portfolio main.go

EXPOSE 8082
CMD ["/bin/portfolio"]