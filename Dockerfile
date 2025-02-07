# Python 3.9 Slim tabanlı bir Docker imajı kullanıyoruz
FROM python:3.9-slim

# Çalışma dizinini ayarla
WORKDIR /app

# Gerekli bağımlılık dosyasını kopyala
COPY requirements.txt .

# Bağımlılıkları yükle
RUN pip install --no-cache-dir -r requirements.txt

# Ana Python dosyasını kopyala
COPY main.py .

# Uygulamayı başlat
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
