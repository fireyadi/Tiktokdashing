FROM apify/actor-python:3.11

COPY .actor/requirements.txt /usr/src/app/requirements.txt

RUN pip install --no-cache-dir -r /usr/src/app/requirements.txt \
    && python -m playwright install --with-deps chromium

COPY . /usr/src/app

CMD ["python", "-u", "main.py"]
