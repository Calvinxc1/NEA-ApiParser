FROM python:3-slim

RUN apt-get update \
    && apt-get install -y git \
    && python -m pip install --upgrade pip

WORKDIR /srv/app

ARG schema_branch=develop
RUN git clone -b $schema_branch https://github.com/Calvinxc1/NEA-Schema.git \
    && python -m pip install --no-cache-dir ./NEA-Schema \
    && rm -rf ./NEA-Schema
    
COPY ./requirements.txt ./
RUN pip install -r requirements.txt \
    && rm requirements.txt

COPY ./config ./config/
COPY ./run.py ./
COPY ./nea_EsiParser ./nea_EsiParser/

CMD ["python", "run.py"]
