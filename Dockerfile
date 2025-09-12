# Builder Stage
FROM rust:1.86-bookworm AS builder

# Build argument to decide which binary to include in this image
ARG BINARY_NAME
RUN if [ -z "$BINARY_NAME" ]; then \
      echo >&2 "ERROR: you must set BINARY_NAME env"; \
      exit 1; \
    fi

# Set the working directory
WORKDIR /app

# Copy workspace Cargo files first
COPY Cargo.toml Cargo.lock ./
COPY Cargo.lock ./

# Create dummy files for each workspace member to cache dependencies
RUN mkdir -p src/bin/scripts src/bin/recovery && \
    echo 'fn main() {}' > src/bin/price_feed.rs && \
    echo 'fn main() {}' > src/bin/scripts/queue_migration.rs && \
    echo 'fn main() {}' > src/bin/recovery/proof_retrier.rs && \
    echo 'fn main() {}' > src/bin/recovery/dlq_recovery.rs


# Build dependencies (this will cache them)
RUN cargo build --release

# Remove the dummy files
RUN rm -rf src/

# Now copy the actual source code
COPY src/ ./src/

# Build the project with actual source code
RUN cargo build --release --bin ${BINARY_NAME};

# Final Stage: Produce a lean runtime image
FROM debian:bookworm-slim

# Install runtime dependencies and clean up in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Set the base path environment variable
ENV BASE_PATH=/app

# Copy config and certs
COPY certs/ ./certs/
COPY config/ ./config/

# Build argument to decide which binary to include in this image
ARG BINARY_NAME
RUN if [ -z "$BINARY_NAME" ]; then \
      echo >&2 "ERROR: you must set BINARY_NAME env"; \
      exit 1; \
    fi
ENV BINARY_NAME=${BINARY_NAME}

# Copy the desired binary from the builder stage
COPY --from=builder /app/target/release/${BINARY_NAME} /usr/local/bin/${BINARY_NAME}

# Run the selected binary
ENTRYPOINT /usr/local/bin/$BINARY_NAME