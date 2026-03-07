FROM python:3.12.11-alpine3.22

WORKDIR /

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt /sfme/requirements.txt
RUN pip install -r /sfme/requirements.txt

COPY . /sfme

CMD ["python3", "-m", "sfme"]
