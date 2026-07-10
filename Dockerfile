FROM archlinux:latest

LABEL maintainer="kioto"
LABEL description="Development environment for kioto"

WORKDIR /workspace/kioto

RUN pacman -Syu --noconfirm && \
    pacman -S --noconfirm \
    base-devel \
    gcc \
    clang \
    llvm \
    cmake \
    ninja \
    pkgconf \
    git \
    curl \
    wget \
    ca-certificates \
    gdb \
    lldb \
    liburing \
    vim \
    && pacman -Scc --noconfirm

COPY . /workspace/kioto

RUN cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DKIOTO_BUILD_EXAMPLES=ON \
    -DKIOTO_SENDERS_BACKEND=NVIDIA \
    && cmake --build build -j$(nproc)

EXPOSE 8080 8086

CMD ["/bin/bash"]