FROM ibm-semeru-runtimes:open-11-jre
WORKDIR /app
COPY target/mkmconnector-fatjar.jar /app

CMD [ "/bin/sh", "-c", "java -Xmx64m -jar mkmconnector-fatjar.jar 2>&1 | tee logs/full.logs" ]