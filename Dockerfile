FROM python
RUN pip3 install boto3
RUN pip3 install pandas
ADD S3_file_processor.py /
CMD [ "python", "./S3_file_processor.py" ]
