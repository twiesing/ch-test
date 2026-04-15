FROM node:22-alpine
WORKDIR /app
RUN npm i -g tsx
COPY server.ts .
EXPOSE 3333
CMD ["tsx", "server.ts"]
