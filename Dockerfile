# =============================================================================
# Senzing Service Bus Consumer
# =============================================================================
# Dockerfile for running sz_sb_consumer with Azure Service Bus and Azure SQL
# Based on senzingsdk-runtime with MSSQL ODBC drivers and Python dependencies
# =============================================================================

ARG BASE_IMAGE=public.ecr.aws/senzing/senzingsdk-runtime:staging
FROM ${BASE_IMAGE}

USER root

# Install Python, MSSQL ODBC drivers, and dependencies
RUN apt-get update && \
    apt-get -y install apt-transport-https wget curl gnupg python3 python3-pip python3-venv && \
    # Add Microsoft package repository for MSSQL tools (Debian 13/trixie uses Debian 12 packages)
    # Use /usr/share/keyrings for the GPG key (modern Debian approach)
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql18 mssql-tools18 && \
    # Create venv with access to system packages (for senzing_core)
    python3 -m venv --system-site-packages /app/venv && \
    /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install orjson azure-servicebus azure-identity senzing senzing_core && \
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
