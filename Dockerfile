# STL Search — run on Synology (Container Manager)
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STL_HOST=0.0.0.0 \
    STL_PORT=8787

WORKDIR /app

# RAR extract needs the proprietary unrar binary (non-free) and/or 7-Zip.
RUN sed -i -E 's/Components: (.*)/Components: \1 contrib non-free non-free-firmware/' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        p7zip-full \
        unrar \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY run.py .
COPY channels.example.txt ./channels.txt

RUN mkdir -p /app/data /downloads

EXPOSE 8787

CMD ["python", "run.py"]
