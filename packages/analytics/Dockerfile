FROM node:20-alpine
WORKDIR /app

COPY packages/shared/package.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src
COPY packages/shared/tsconfig.json ./packages/shared/

COPY packages/analytics/package.json ./packages/analytics/
COPY packages/analytics/src ./packages/analytics/src
COPY packages/analytics/tsconfig.json ./packages/analytics/

COPY package.json package-lock.json ./

RUN npm install --workspaces --include-workspace-root
RUN cd packages/shared && npx tsc
RUN cd packages/analytics && npx tsc

EXPOSE 3002
CMD ["node", "packages/analytics/dist/index.js"]
