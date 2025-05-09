# Use Python 3.9 as the base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy all project files to the container
COPY . /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the FastAPI default port
EXPOSE 8000

# Start FastAPI (assuming main.py contains `uvicorn.run(app, host="0.0.0.0", port=8000)`)
CMD ["python", "main.py"]
