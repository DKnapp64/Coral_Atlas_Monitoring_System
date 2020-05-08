FROM ubuntu:18.04

RUN apt update && \
    apt install -y software-properties-common && \
    apt-add-repository ppa:ubuntugis/ppa && \
    apt-add-repository ppa:deadsnakes/ppa && \
    apt update && \
    apt install -y \
    git \
    ssh \
    gdal-bin \
    libgdal-dev \
    python3.8 \
    python3.8-dev \
    python3-pip

ARG GITHUB_DEPLOY_KEY
RUN mkdir /root/.ssh
RUN echo "${GITHUB_DEPLOY_KEY}" >> /root/.ssh/id_rsa && chmod 600 /root/.ssh/id_rsa

RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN pip3 install pipenv

WORKDIR /app
COPY . .

# Ref https://click.palletsprojects.com/en/7.x/python3/
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pipenv install --dev --sequential

RUN rm /root/.ssh/id_rsa

ENV PATH=/app:${PATH}
ENV PYTHONPATH=/app
