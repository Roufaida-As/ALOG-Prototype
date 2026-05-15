FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY terrain_layer/ .
CMD ["python", "edge_node.py"]