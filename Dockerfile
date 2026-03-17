# 1. Grab Java from the official source (standard JDK 21 image)
FROM eclipse-temurin:21-jdk AS java_base

# 2. Start our actual Python environment
FROM python:3.11-slim-bookworm

# 3. Copy Java from the first image to this one
COPY --from=java_base /opt/java/openjdk /opt/java/openjdk

# 4. Set Environment Variables
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 5. Install system utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 6. Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]