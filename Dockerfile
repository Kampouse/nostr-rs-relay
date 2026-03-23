FROM rust:1.88 as builder

WORKDIR /usr/src/app

COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

RUN mkdir -p /usr/src/app/db

COPY --from=builder /usr/src/app/target/release/nostr-rs-relay /usr/src/app/nostr-rs-relay
COPY config.toml /usr/src/app/config.toml

ENV PORT=8080

EXPOSE 8080

CMD ["./nostr-rs-relay"]
