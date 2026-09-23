FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/dashboard/package.json ./packages/dashboard/
RUN npm install --workspaces --include-workspace-root --ignore-scripts
COPY packages/dashboard ./packages/dashboard
RUN cd packages/dashboard && npm run build

FROM nginx:alpine
COPY --from=builder /app/packages/dashboard/dist /usr/share/nginx/html
COPY packages/dashboard/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
