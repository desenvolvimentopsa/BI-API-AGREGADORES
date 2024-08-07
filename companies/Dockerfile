FROM python:3-slim

WORKDIR /app
RUN mkdir logs
COPY requirements.txt ./
RUN pip install Unidecode
RUN  pip install --no-cache-dir -r requirements.txt
COPY src .
COPY utils .

CMD [ "python", "companies.py"]