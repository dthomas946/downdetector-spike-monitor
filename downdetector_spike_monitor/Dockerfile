ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.12-alpine3.20
FROM $BUILD_FROM
WORKDIR /app
COPY requirements.txt .
RUN apk add --no-cache gcc musl-dev libffi-dev \
 && pip install --no-cache-dir -r requirements.txt \
 && apk del gcc musl-dev libffi-dev
COPY app ./app
COPY watchlist.example.yaml .
COPY run.sh /run.sh
RUN chmod +x /run.sh
EXPOSE 8099
CMD ["/run.sh"]
