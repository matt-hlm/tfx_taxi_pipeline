FROM tensorflow/tfx:1.10.0
WORKDIR /pipeline

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./ ./
ENV PYTHONPATH="/pipeline:${PYTHONPATH}"

