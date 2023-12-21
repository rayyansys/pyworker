FROM python:2.7.18-buster

LABEL maintainer="Hossam Hammady <hossam@rayyan.ai>"

WORKDIR /home

# install deps first
RUN pip install --upgrade pip
COPY requirements-test.txt /home/
RUN pip install -r requirements-test.txt

# copy rest of files
COPY / /home/

CMD ["pytest"]
