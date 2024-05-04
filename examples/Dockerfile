FROM python:3.10.14-slim

WORKDIR /app

RUN python -m venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH=${VIRTUAL_ENV}/bin:${PATH}

RUN pip install --upgrade pip

ADD examples/requirements.txt .

RUN pip install -r requirements.txt

ADD *.py pyproject.toml README.md ./

RUN pip install -e .
