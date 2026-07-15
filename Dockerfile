# STL Search — run on Synology (Container Manager)
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STL_HOST=0.0.0.0 \
    STL_PORT=8787

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY run.py .
COPY channels.example.txt ./channels.txt

RUN mkdir -p /app/data /downloads

EXPOSE 8787

CMD ["python", "run.py"]
