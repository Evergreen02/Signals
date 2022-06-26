ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=11
ARG PYTHON_VERSION=3.10.5

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED 1

WORKDIR /code
COPY ./runner.py /code/

ARG PYSPARK_VERSION=3.3.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

ENTRYPOINT ["python", "runner.py"]