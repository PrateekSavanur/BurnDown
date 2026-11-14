FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 DB_PATH=/data/burndown.db PORT=8088
RUN pip install --no-cache-dir tzdata && useradd -r -u 10001 burndown && mkdir /data && chown burndown /data
WORKDIR /app
COPY server.py ./
COPY static ./static
USER burndown
VOLUME /data
EXPOSE 8088
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request,os;urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/healthz')" || exit 1
CMD ["python", "server.py"]
