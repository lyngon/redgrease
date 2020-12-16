FROM python:3
ARG app_name=redgrease

WORKDIR /usr/src/app

COPY . .

RUN apt-get update \
&& apt-get install -y \
    apt-utils \
    netcat \
&& python -m pip install --upgrade pip \
&& pip install --no-cache-dir -r requirements.txt  

ENTRYPOINT ["redgrease"]
