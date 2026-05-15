# Build Stage
FROM golang:1.24-alpine AS builder
WORKDIR /app

# Install git for downloading dependencies
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o worker .

# Execution Stage (Minimal Image)
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/worker .

CMD ["./worker"]