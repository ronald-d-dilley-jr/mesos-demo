FROM python:3.6.4-alpine3.7


# Update the OS packages to the latest versions
RUN apk update --no-cache


# Install packages we want to use
#RUN apk add --no-cache bash python3
#RUN hash -r


# Install python libraries we need
RUN pip3 install pika


COPY work-manager.py /bin/work-manager.py
ENTRYPOINT /bin/work-manager.py
