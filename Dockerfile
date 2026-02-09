FROM python:3.11-slim-bookworm

# Install Java and System dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install all Python libraries at once
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code
COPY . .

# Since you want to separate the project, 
# you can change the default command to your pipeline script
CMD ["python3", "run_pipeline.py"]