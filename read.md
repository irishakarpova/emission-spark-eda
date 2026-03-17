# 1. Build the image and tag it as 'vin-pipeline'

docker build -t vin-pipeline .

# 2. Remove any existing container with this name to avoid conflicts

docker rm -f vin-app || true && docker

# 3. Run the container

run --rm -p 8501:8501 \
 --name vin-pipeline \
 --memory="12g" \
 --shm-size="2g" \
 -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/app.py:/app/app.py" \
 -v "$(pwd)/engine.py:/app/engine.py" \
 vin-pipeline
