FROM python:3.14-slim
WORKDIR /app

COPY requirements.txt .

RUN python3 -m pip install -r requirements.txt
RUN apt update && apt install -y fuse libfuse2 procps

COPY tgdrive ./tgdrive

CMD ["python3", "-m", "tgdrive", "--foreground", "/app/mount"]
