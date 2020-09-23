FROM python:3.7.2
ADD ./consumer/ /code
ADD ./vars/ /code
WORKDIR /code
RUN pip install -r requirements.txt
CMD ["python","consumer.py"]