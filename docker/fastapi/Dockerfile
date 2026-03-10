FROM python:3.11-slim

WORKDIR /app

COPY docker/fastapi/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY serving/ serving/

EXPOSE 8000

CMD ["uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
