FROM node:18-bookworm

ARG TINI_VER="v0.19.0"

# install tini
ADD https://github.com/krallin/tini/releases/download/$TINI_VER/tini /sbin/tini
RUN chmod +x /sbin/tini

# copy minetrack files
WORKDIR /usr/src/minetrack
COPY . .

# install dependencies
RUN npm install --build-from-source

# copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# build dist at container start to ensure fresh build
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]

EXPOSE 8080

CMD ["node", "main.js"]

EXPOSE 8080

ENTRYPOINT ["/sbin/tini", "--", "node", "main.js"]
