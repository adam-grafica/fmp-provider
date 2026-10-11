# FMP — private development inference adapter. Localhost-only default.
# Build: docker build -t fmp-provider:2.3.0 .
# Run:   docker run --rm -p 127.0.0.1:2089:2089 --env-file .env fmp-provider:2.3.0
FROM node:22-alpine
WORKDIR /app
COPY package.json server.js ./
COPY lib/ ./lib/
COPY bin/ ./bin/
RUN node --check server.js && node bin/fmp.js doctor || true
# Non-root user; no host mounts, no docker socket, no capabilities.
RUN addgroup -S fmp && adduser -S fmp -G fmp && chown -R fmp:fmp /app
USER fmp
ENV FMP_HOST=127.0.0.1 FMP_PORT=2089
EXPOSE 2089
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD node bin/fmp.js health || exit 1
CMD ["node", "server.js"]
