FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data

EXPOSE 8050

# Runs both collector (background) and dashboard (foreground)
CMD ["sh", "-c", "python collector.py & python live_collector.py & python -m gunicorn -w 2 -b 0.0.0.0:8050 dashboard:app"]
