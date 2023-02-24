FROM python:3.8

RUN apt-get update && \
    apt-get install -y openjdk-11-jre && \
    apt-get install -y ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/ && \
    rm -rf /var/cache/oracle-jdk11-installer;
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

COPY requirements.txt requirements.txt 

RUN pip install -r requirements.txt

WORKDIR /app

COPY . /app

EXPOSE 4050

CMD ["python", "./src/main.py"]
