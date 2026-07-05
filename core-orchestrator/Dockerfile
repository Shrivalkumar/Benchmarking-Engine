FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json tsconfig.json ./
RUN npm install
COPY src ./src
RUN npm run build

FROM node:20-alpine
WORKDIR /app

# Install docker-cli so the orchestrator can build contestant images
RUN apk add --no-cache docker-cli

COPY package*.json ./
RUN npm install --omit=dev
COPY --from=builder /app/dist ./dist

# Create temp build directory inside container
RUN mkdir -p temp_builds

EXPOSE 8000
CMD ["node", "dist/index.js"]
