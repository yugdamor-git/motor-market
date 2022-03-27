# start by pulling the python image
FROM ubuntu

RUN apt-get update
RUN apt-get install -y python3-pip python3-dev build-essential libssl-dev libffi-dev python3-setuptools

# copy the requirements file into the image
COPY ./src/requirements.txt /app/requirements.txt

# switch working directory
WORKDIR /app

# install the dependencies and packages in the requirements file
RUN pip install -r requirements.txt

# copy every content from the local file to the image
COPY ./src /app

# configure the container to run in an executed manner
ENTRYPOINT [ "uwsgi" ]

CMD ["--ini","uwsgi.ini"]