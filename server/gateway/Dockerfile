# ── Fase 1: Build ──────────────────────────────────────────────────────────
FROM node:20-alpine AS builder
WORKDIR /app

# package.json está em server/gateway/ relativo ao context (raiz do projeto)
COPY server/gateway/package*.json ./
RUN npm install && npm cache clean --force

# tsconfig e src também em server/gateway/
COPY server/gateway/tsconfig.json ./
COPY server/gateway/src ./src

# proto está na raiz do projeto
COPY agro_telemetry.proto ./agro_telemetry.proto

# Instala devDependencies (ts-node, typescript) e compila
RUN npm install --save-dev typescript @types/node @types/amqplib @types/uuid && \
    npx tsc

# ── Fase 2: Runtime ─────────────────────────────────────────────────────────
FROM node:20-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production

COPY --from=builder /app/node_modules          ./node_modules
COPY --from=builder /app/dist                  ./dist
COPY --from=builder /app/agro_telemetry.proto  ./agro_telemetry.proto

EXPOSE 50051
CMD ["node", "dist/index.js"]
