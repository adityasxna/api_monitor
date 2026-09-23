FROM node:20-alpine
WORKDIR /app

COPY packages/shared/package.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src
COPY packages/shared/tsconfig.json ./packages/shared/

COPY packages/traffic-generator/package.json ./packages/traffic-generator/
COPY packages/traffic-generator/src ./packages/traffic-generator/src
COPY packages/traffic-generator/tsconfig.json ./packages/traffic-generator/

COPY package.json package-lock.json ./

RUN npm install --workspaces --include-workspace-root
RUN cd packages/shared && npx tsc
RUN cd packages/traffic-generator && npx tsc

CMD ["node", "packages/traffic-generator/dist/index.js"]
