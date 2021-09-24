from maven:3.3-jdk-8

USER root

#COPY ./cacerts /etc/ssl/certs/java/cacerts
#COPY ./cacerts /etc/default/cacerts

RUN mkdir -p /srv/.m2
RUN chmod -R 777 /srv/.m2
RUN chmod -R 777 /usr/local
RUN useradd -ms /bin/bash wasadm


VOLUME ["/srv/.m2"]
ENV MAVEN_CONFIG=/srv/.m2

RUN rm -rf /usr/local/boot/salesActive
RUN mkdir -p /usr/local/boot/salesActive

RUN chown -R wasadm:wasadm /usr/local/boot/salesActive

USER wasadm

COPY . /usr/local/boot/salesActive

RUN cd /usr/local/boot/salesActive

WORKDIR /usr/local/boot/salesActive/
RUN mvn package

#CMD ["java","-Xdebug","-Xnoagent","-Xrunjdwp:transport=dt_socket,address=18002,server=y,suspend=n", "-Djennifer.config=/usr/local/jennifer5/agent.java/conf/salesActive.conf", "-javaagent:/usr/local/jennifer5/agent.java/jennifer.jar", "-jar","/usr/local/boot/salesActive/target/salesActive-0.0.1-SNAPSHOT.jar"]
CMD ["java","-Xdebug","-Xnoagent","-Xrunjdwp:transport=dt_socket,address=18002,server=y,suspend=n", "-jar","/usr/local/boot/salesActive/target/salesActive-0.0.1-SNAPSHOT.jar"]