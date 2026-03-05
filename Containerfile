FROM almalinux:9

# Better logging for Python apps
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN dnf -y update && \
    dnf -y install \
        python3 \
        python3-pip \
        git \
        which \
        procps \
        hostname \
        libcurl-devel \
        vim \
    && dnf clean all

# Install useful development tools
RUN pip install --no-cache-dir \
        debugpy

# Directory where the repo will be mounted
WORKDIR /workspace

# Default shell
CMD ["/bin/bash"]
