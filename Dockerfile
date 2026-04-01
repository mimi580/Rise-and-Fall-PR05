FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first (layer cache)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Run the bot
CMD ["python", "bot.py"]
