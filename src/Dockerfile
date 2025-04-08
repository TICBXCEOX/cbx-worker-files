FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && apt-get clean

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

# executa o dispatcher por padrao (main raiz)
CMD ["python", "main.py"]    