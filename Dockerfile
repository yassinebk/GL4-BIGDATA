FROM ranimmbarek/hadoop-cluster

COPY mapred-site.xml /usr/local/hadoop/etc/hadoop/mapred-site.xml

COPY entrypoint.sh /

RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]