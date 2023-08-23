# Use a base image with Python and PostgreSQL
FROM python:3.8

# Set environment variables for PostgreSQL
ENV POSTGRES_USER myuser
ENV POSTGRES_PASSWORD mypassword
ENV POSTGRES_DB mydb

# Install necessary Python libraries
RUN pip install psycopg2 Flask python-dotenv

# Set the working directory
WORKDIR /app

# Copy your Python script into the container
COPY app.py .
COPY scripts ./scripts

# Expose the PostgreSQL port
EXPOSE 5432