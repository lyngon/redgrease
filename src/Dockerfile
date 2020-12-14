FROM python:3
ARG app_name=redgrease

WORKDIR /usr/src/app

COPY . .

RUN apt-get update \
&& apt-get install -y \
    apt-utils \
    netcat \
&& pip install --no-cache-dir -r requirements.txt \
&& pip install --no-cache-dir -r redgrease/requirements.txt

ENTRYPOINT [ "python3", "loader.py"]
