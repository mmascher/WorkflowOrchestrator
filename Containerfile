FROM almalinux:9

# Better logging for Python apps
ENV PYTHONUNBUFFERED=1

# HTCondor repository
RUN dnf install -y \
https://htcss-downloads.chtc.wisc.edu/repo/25.0/htcondor-release-current.el9.noarch.rpm

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
        condor \
    && dnf clean all

# Install useful development tools
RUN pip install --no-cache-dir \
        debugpy

# Directory where the repo will be mounted
WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Default shell
CMD ["/bin/bash"]
