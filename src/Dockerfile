FROM python:3.11
WORKDIR /code
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
RUN echo "hello there"
RUN pwd
CMD ["python", "main.py"]