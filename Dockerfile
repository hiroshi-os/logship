FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/logship ./cmd/logship \
 && CGO_ENABLED=0 go build -o /out/logship-cli ./cmd/logship-cli \
 && CGO_ENABLED=0 go build -o /out/logship-bench ./cmd/bench

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/logship /usr/local/bin/logship
COPY --from=build /out/logship-cli /usr/local/bin/logship-cli
COPY --from=build /out/logship-bench /usr/local/bin/logship-bench
EXPOSE 9092
ENTRYPOINT ["logship"]
