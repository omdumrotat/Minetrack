FROM node:18-bookworm

ARG TINI_VER="v0.19.0"

# install tini
ADD https://github.com/krallin/tini/releases/download/$TINI_VER/tini /sbin/tini
RUN chmod +x /sbin/tini

# install sqlite3
RUN apt-get update                                                   \
 && apt-get install    --quiet --yes --no-install-recommends sqlite3 \
 && apt-get clean      --quiet --yes                                 \
 && apt-get autoremove --quiet --yes                                 \
 && rm -rf /var/lib/apt/lists/*

# copy minetrack files
WORKDIR /usr/src/minetrack
COPY . .

# build minetrack
RUN npm install --build-from-source \
 && npm run build

EXPOSE 8080

ENTRYPOINT ["/sbin/tini", "--", "node", "main.js"]
