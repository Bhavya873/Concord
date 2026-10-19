FROM rust:1.98.1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p concord-sim --bin concord-demo

FROM debian:bookworm-slim
COPY --from=build /src/target/release/concord-demo /usr/local/bin/concord-demo
ENV PORT=8766
EXPOSE 8766
CMD ["concord-demo"]
