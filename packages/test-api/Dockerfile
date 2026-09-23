FROM node:20-alpine
WORKDIR /app

# Copy shared package first (dependency)
COPY packages/shared/package.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src
COPY packages/shared/tsconfig.json ./packages/shared/

# Copy service package
COPY packages/test-api/package.json ./packages/test-api/
COPY packages/test-api/src ./packages/test-api/src
COPY packages/test-api/tsconfig.json ./packages/test-api/

COPY package.json package-lock.json ./

RUN npm install --workspaces --include-workspace-root
RUN cd packages/shared && npx tsc
RUN cd packages/test-api && npx tsc

EXPOSE 3001
CMD ["node", "packages/test-api/dist/index.js"]
