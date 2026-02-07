
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    streamlit \
    pandas \
    pyarrow

COPY . .

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]

# ... (existing setup)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# ...