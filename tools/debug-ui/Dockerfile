FROM node:19-alpine AS build

WORKDIR /build
ADD package.json /build/package.json
ADD package-lock.json /build/package-lock.json
RUN npm install

ADD src /build/src
ADD public /build/public
ADD tsconfig.json /build/tsconfig.json
RUN npm run build

# Serving does not require npm; simple nginx is good enough; it's just some
# static files.
FROM nginx:1.21-alpine
COPY --from=build /build/build /var/www/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
