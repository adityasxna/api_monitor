FROM node:20-alpine
WORKDIR /app

COPY packages/shared/package.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src
COPY packages/shared/tsconfig.json ./packages/shared/

COPY packages/gateway/package.json ./packages/gateway/
COPY packages/gateway/src ./packages/gateway/src
COPY packages/gateway/tsconfig.json ./packages/gateway/

COPY package.json package-lock.json ./

RUN npm install --workspaces --include-workspace-root
RUN cd packages/shared && npx tsc
RUN cd packages/gateway && npx tsc

EXPOSE 3000
CMD ["node", "packages/gateway/dist/index.js"]
