FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno (recommended JS runtime for yt-dlp EJS challenge solver, v2.0+)
RUN curl -fsSL https://deno.land/install.sh | sh
ENV DENO_INSTALL="/root/.deno"
ENV PATH="$DENO_INSTALL/bin:$PATH"
RUN deno --version

# Install yt-dlp and bgutil plugin in the same Python env
RUN pip install --no-cache-dir yt-dlp bgutil-ytdlp-pot-provider flask requests

COPY app.py .
COPY static/ ./static/

RUN mkdir -p /app/downloads /app/logs

EXPOSE 5000

CMD ["python", "app.py"]
