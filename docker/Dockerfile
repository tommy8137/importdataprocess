FROM node:8.12.0-alpine

RUN mkdir -p /kafka-client

ADD package*.json /kafka-client/
ADD server.js /kafka-client
ADD config.js /kafka-client
ADD common.js /kafka-client
ADD kafka_modules/ /kafka-client/kafka_modules
ADD common/ /kafka-client/common
ADD schedule_modules/ /kafka-client/schedule_modules
ADD utils/ /kafka-client/utils
ADD server/ /kafka-client/server

WORKDIR /kafka-client
RUN apk add --no-cache --virtual .gyp \
        python \
        make \
        g++ \
    && npm install \
    && apk del .gyp

# exec app
CMD [ "npm", "start" ]
