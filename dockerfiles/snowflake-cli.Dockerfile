FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-jdbc
LABEL org.opencontainers.image.description="Image with the latest snowflake-cli package"
RUN pip install --upgrade pip
RUN pip install --no-cache snowflake-cli
