FROM debian

ENV AGENTE_HOST=agente
ENV PYTHONUNBUFFERED=1

WORKDIR /app
RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-psycopg2 openssh-server sudo -y
RUN useradd -rm -d /home/ubuntu -s /bin/bash -g root -G sudo -u 999 test 
env | egrep -v "^(HOME=|USER=|MAIL=|LC_ALL=|LS_COLORS=|LANG=|HOSTNAME=|PWD=|TERM=|SHLVL=|LANGUAGE=|_=)" >> /etc/environment

RUN echo 'test:test' | chpasswd
RUN service ssh start

COPY . .

CMD ["/usr/sbin/sshd","-D"]
