FROM python:3
ARG app_name=redgrease

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apt update \
&& apt get install -y netcat\
&& pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "watcher.py"]