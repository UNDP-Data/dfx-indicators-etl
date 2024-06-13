FROM python:3.10-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install dependencies
RUN pip install --upgrade pip

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

CMD ["--help", "-h", "run", "-l", "--log-level", "-e", "--load-env-file", "-i", "--indicators", "-s", "--sources", "-c", "--config", "-f", "--filter-indicators-string"]
