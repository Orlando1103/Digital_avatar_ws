FROM adoptopenjdk/openjdk14
MAINTAINER zhangwenshuo m15101166628@163.com

# ADD sources.list /etc/apt/

RUN apt-get update \
    && apt install -y htop \
    && apt install -y curl \
    && apt install -y vim \
    && apt install -y net-tools \
    && apt install -y iputils-ping \
    && apt install -y pandoc

ENV LANG C.UTF-8
# Set the location of the verticles
ENV APP_HOME /app

EXPOSE 8080

VOLUME $APP_HOME/files
VOLUME $APP_HOME/tmp
VOLUME $APP_HOME/cache
VOLUME $APP_HOME/logs
VOLUME $APP_HOME/user_files

COPY misscut-leader-check-server-*-fat.jar $APP_HOME/misscut-leader-check-server.jar
COPY config.yml $APP_HOME/config.yml

# Launch the app
WORKDIR $APP_HOME

CMD echo "miscut web service starting ...."
ENTRYPOINT ["java", "-Duser.timezone=GMT+8","-jar", "misscut-leader-check-server.jar", "-c", "config.yml"]

