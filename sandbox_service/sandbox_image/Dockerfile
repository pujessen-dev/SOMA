FROM python:3.11-slim

WORKDIR /sandbox

COPY requirements.txt /sandbox/requirements.txt
RUN pip install --no-cache-dir -r /sandbox/requirements.txt

COPY run_code.py /sandbox/run_code.py

ENTRYPOINT ["python", "/sandbox/run_code.py"]