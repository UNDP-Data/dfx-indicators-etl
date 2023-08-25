FROM python:3.10-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install dependencies
RUN pip install --upgrade pip

## Install any needed packages specified in requirements.txt
# RUN pip install -r requirements.txt

# Install the package in editable mode (including dependencies)
RUN pip install -e .

ENTRYPOINT ["python", "dfpp/cli.py"]

CMD ["--help", "-h", "run", "-l", "--log-level", "-e", "--loa
d-env-file", "-i", "--indicators", "-s", "--sources", "-c", "--config", "-f", "--filter-indicators-string"]