FROM python:3.7.2
ADD ./producer/ /code
ADD ./vars/ /code
WORKDIR /code
RUN pip3 install -r requirements.txt
CMD ["python","producer.py"]