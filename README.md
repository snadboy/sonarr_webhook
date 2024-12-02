# Sonarr Webhook Listener

A Python application that listens for Sonarr webhook events and processes them with proper logging.

## Features

- Webhook endpoint for Sonarr events
- Configurable logging levels
- Environment-based configuration
- Support for various Sonarr event types:
  - Download events
  - Grab events
  - Rename events

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and configure your settings:
   ```bash
   cp .env.example .env
   ```
4. Edit `.env` file with your Sonarr details:
   - `SONARR_API_KEY`: Your Sonarr API key
   - `SONARR_URL`: Your Sonarr instance URL
   - `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)

## Usage

Run the application:
```bash
python main.py
```

The webhook endpoint will be available at:
```
http://your-server:8000/webhook
```

Configure this URL in your Sonarr's Connect settings as a webhook connection.

## Endpoints

- `POST /webhook`: Receives Sonarr webhook events
- `GET /health`: Health check endpoint

## Logging

The application uses Python's built-in logging module. Log level can be configured through the `LOG_LEVEL` environment variable.
