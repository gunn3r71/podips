FROM golang:1.16-alpine

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /podips-mock-producer

CMD ["/podips-mock-producer"]