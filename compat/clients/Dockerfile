# Real IMAP clients for the compatibility runs (compat/README.md).
FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends isync offlineimap3 python3 nodejs npm ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/node
RUN npm install --no-audit --no-fund imapflow@2.11.0
ENV NODE_PATH=/opt/node/node_modules
WORKDIR /work
