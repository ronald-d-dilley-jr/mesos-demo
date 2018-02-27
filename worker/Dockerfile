FROM python:3.6.4-alpine3.7


# Update the OS packages to the latest versions
RUN apk update --no-cache


COPY demo-worker.py /usr/local/bin


ENTRYPOINT ["/usr/local/bin/demo-worker.py"]
