# Leveraging the pre-built Docker images with
# cargo-chef and the Rust toolchain
# https://www.lpalmieri.com/posts/fast-rust-docker-builds/
FROM --platform=${BUILDPLATFORM:-linux/amd64} lukemathwalker/cargo-chef:latest-rust-1.85.0 AS chef
WORKDIR /coach

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder

WORKDIR /coach

COPY --from=planner /coach/recipe.json recipe.json

RUN apt-get update && apt-get install -y clang cmake protobuf-compiler && rustup component add rustfmt

# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .

# Build actual target here
RUN cargo build --release

RUN mv target/release/coach /coach/coach

FROM debian:12-slim
ARG APP=/coach

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Etc/UTC \
    RUN_MODE=production

RUN mkdir -p ${APP}

COPY --from=builder /coach/coach ${APP}/coach

WORKDIR ${APP}

CMD ["./coach"]
