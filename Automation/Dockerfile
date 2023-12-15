#
FROM python:3.11

#
WORKDIR /app

#
COPY requirements.txt /app/

#
RUN pip install --no-cache-dir -r requirements.txt

#
COPY app /app/
 
# 預設執行的指令
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8888"]
