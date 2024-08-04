#SET Base image
FROM python:3.9

#Set working directory in the container
WORKDIR /app

#Copy the dependencies
COPY file_watcher/__init.py .
COPY file_watcher/requirements.txt .

#Install dependencies
RUN pip install -r requrements.txt

#Copy file Watcher code
COPY file_watcher/scripts/ .

ENV PYTHONPATH="${PYTHONPATH}:/app"

CMD [ "python", "-m", "file_watcher" ]