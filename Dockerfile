FROM python:3.11

LABEL maintainer="Hossam Hammady <github@hammady.net>"

WORKDIR /home

COPY / /home/

RUN pip install --upgrade pip && \
    pip install twine && \
    python setup.py sdist bdist_wheel

CMD ["twine", "upload", "dist/*"]
