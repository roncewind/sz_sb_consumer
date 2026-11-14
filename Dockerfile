# =============================================================================
# Senzing Service Bus Consumer
# =============================================================================
# Dockerfile for running sz_sb_consumer with Azure Service Bus and Azure SQL
# Based on senzingsdk-runtime with MSSQL ODBC drivers and Python dependencies
# =============================================================================

ARG BASE_IMAGE=public.ecr.aws/senzing/senzingsdk-runtime:staging
FROM ${BASE_IMAGE}

USER root

# Pinned Python deps (see requirement.txt for why: an unpinned azure-servicebus bump cost ~2.5x throughput)
COPY requirement.txt /tmp/requirement.txt

# Install Python, MSSQL ODBC drivers, and dependencies
RUN . /etc/os-release && \
    apt-get update && \
    apt-get -y install curl python3 python3-pip python3-venv && \
    # Add Microsoft's official repo + signing key for this Debian release.
    # See Dockerfile.redoer for why we use the config package instead of microsoft.asc.
    curl -fsSL -O "https://packages.microsoft.com/config/debian/${VERSION_ID}/packages-microsoft-prod.deb" && \
    dpkg -i packages-microsoft-prod.deb && \
    rm -f packages-microsoft-prod.deb && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql18 mssql-tools18 && \
    # Create venv with access to system packages (for senzing_core)
    python3 -m venv --system-site-packages /app/venv && \
    /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install --no-cache-dir -r /tmp/requirement.txt && \
    # Cleanup
    apt-get -y clean && rm -rf /var/lib/apt/lists/*

COPY sz_sb_consumer.py /app/
RUN chmod +x /app/sz_sb_consumer.py

# Environment variables for Senzing and Python
ENV PYTHONPATH=/opt/senzing/er/sdk/python:/app
ENV PATH="/opt/mssql-tools18/bin:${PATH}"
ENV SENZING_DOCKER_LAUNCHED=true
ENV PYTHONUNBUFFERED=1

USER 1001

WORKDIR /app
ENTRYPOINT ["/app/venv/bin/python3", "/app/sz_sb_consumer.py"]
