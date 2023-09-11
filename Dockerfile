# Use an official Python runtime as a parent image
FROM python:3.9

# The environment variable ensures that the python output is set straight
# to the terminal with out buffering it first
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install any needed packages specified in requirements.txt
#COPY requirements.txt /app/
RUN python -m pip install --upgrade pip
RUN python -m pip install pipenv
RUN pipenv --python 3.9

#RUN python -m pip install --no-cache-dir -r requirements.txt

#CMD ["python", "./main.py"]