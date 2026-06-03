# syntax=docker/dockerfile:1

# Use the official Go image as the base image
FROM golang:1.24.1 AS build

# Wasmedge Install Dependencies
RUN apt update && apt install -y git python3

RUN useradd -m app

USER app

# Set the working directory inside the container
WORKDIR /home/app/app

# review6 H7: pin install.sh to the immutable release tag and verify its
# sha256 before piping to bash. Mirrors the main vsc-node Dockerfile (see
# go-vsc-node/Dockerfile:16-21). The `master` URL is mutable — any future
# compromise of the WasmEdge GitHub branch would silently pivot every
# mapping-bot build to attacker-controlled install logic. Tagged URL +
# checksum verification removes the supply-chain RCE class for this layer.
RUN curl -sSfL https://raw.githubusercontent.com/WasmEdge/WasmEdge/0.13.4/utils/install.sh -o /tmp/wasmedge-install.sh && \
    echo "89460d9ea15f097e2831c099ee8adb6975b9ffff8a919b33874a655cd54420c0  /tmp/wasmedge-install.sh" | sha256sum -c - && \
    bash /tmp/wasmedge-install.sh -v 0.13.4 && \
    rm -f /tmp/wasmedge-install.sh

# Copy the Go module files
COPY go.mod go.sum ./

# Download the dependencies
RUN go mod download

# Copy the rest of the application code
COPY --chown=app:app . .

# Source the WasmEdge environment and build the application
RUN . /home/app/.wasmedge/env && go build -buildvcs=false -ldflags "-X vsc-node/modules/announcements.GitCommit=$(git rev-parse HEAD)" -o mapping-bot vsc-node/cmd/mapping-bot



FROM rockylinux:9.3-minimal

RUN useradd -m app

USER app

# Set the working directory inside the container
WORKDIR /home/app/app

# Copy the binary from the build stage
COPY --from=build /home/app/app/mapping-bot .

# Copy the WasmEdge from the build stage
COPY --from=build /home/app/.wasmedge /home/app/.wasmedge

# Set environment variables for WasmEdge
ENV LD_LIBRARY_PATH=/home/app/.wasmedge/lib:$LD_LIBRARY_PATH
ENV PATH=/home/app/.wasmedge/bin:$PATH

# Create entrypoint script using sh instead of bash
RUN printf '#!/bin/sh\n\
. /home/app/.wasmedge/env\n\
exec "$@"\n' > /home/app/app/entrypoint.sh && \
chmod +x /home/app/app/entrypoint.sh

# Command to run when the container starts
ENTRYPOINT ["/home/app/app/entrypoint.sh", "./mapping-bot"]
