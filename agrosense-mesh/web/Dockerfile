FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm install && npm cache clean --force

COPY tsconfig.json ./
COPY src ./src

RUN npm install --save-dev typescript @types/node @types/pg @types/ws && \
    npx tsc

FROM node:20-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist         ./dist
COPY src/public                       ./src/public

EXPOSE 3000
CMD ["node", "dist/index.js"]
