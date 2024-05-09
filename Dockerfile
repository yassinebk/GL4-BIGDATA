FROM ranimmbarek/hadoop-cluster

COPY mapred-site.xml /usr/local/hadoop/etc/hadoop/mapred-site.xml

COPY entrypoint.sh /
COPY ./dotfiles/.tmux.conf /root/.tmux.conf

RUN chmod +x /entrypoint.sh

RUN apt update && apt install -y netcat-traditional python3-pip tmux 

RUN pip3 install confluent_kafka flask cassandra-driver

CMD ["/entrypoint.sh"]