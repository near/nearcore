FROM node:22-alpine AS build

WORKDIR /build
ADD package.json /build/package.json
ADD package-lock.json /build/package-lock.json
RUN npm install

ADD src /build/src
ADD index.html /build/index.html
ADD tsconfig.json /build/tsconfig.json
ADD vite.config.ts /build/vite.config.ts
RUN npm run build

# Serving does not require npm; simple nginx is good enough; it's just some
# static files.
FROM nginx:1.25.4-alpine
COPY --from=build /build/build /var/www/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
