FROM node:20-alpine
WORKDIR /app

COPY packages/shared/package.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src
COPY packages/shared/tsconfig.json ./packages/shared/

COPY packages/rca-agent/package.json ./packages/rca-agent/
COPY packages/rca-agent/src ./packages/rca-agent/src
COPY packages/rca-agent/tsconfig.json ./packages/rca-agent/

COPY package.json package-lock.json ./

RUN npm install --workspaces --include-workspace-root
RUN cd packages/shared && npx tsc
RUN cd packages/rca-agent && npx tsc

EXPOSE 3003
CMD ["node", "packages/rca-agent/dist/index.js"]
