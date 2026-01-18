FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_weather_data.py .

CMD ["python", "generate_weather_data.py"]
