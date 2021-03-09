FROM python:2.7-jessie

LABEL maintainer="Hossam Hammady <github@hammady.net>"

WORKDIR /home

COPY / /home/

RUN pip install --upgrade pip && \
    pip install twine && \
    python setup.py sdist bdist_wheel

CMD ["twine", "upload", "dist/*"]
