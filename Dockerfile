# Dockerfile for the Python log producer

# 1. Start with a lightweight Python base image
FROM python:3.9-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy the requirements file into the container first
# This is a best practice for Docker layer caching
COPY requirements.txt .

# 4. Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of the application code
COPY . .

# 6. Define the command to run when the container starts
CMD ["python", "log_producer.py"]
