FROM node:20-alpine AS builder
WORKDIR /app

# package.json em client/ relativo ao context raiz
COPY client/package*.json ./
RUN npm install && npm cache clean --force

# tsconfig e src em client/
COPY client/tsconfig.json ./
COPY client/src ./src

# proto na raiz
COPY agro_telemetry.proto ./agro_telemetry.proto

RUN npm install --save-dev typescript @types/node && \
    npx tsc

FROM node:20-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production

COPY --from=builder /app/node_modules         ./node_modules
COPY --from=builder /app/dist                 ./dist
COPY --from=builder /app/agro_telemetry.proto ./agro_telemetry.proto

CMD ["node", "dist/index.js"]
