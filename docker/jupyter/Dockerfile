# JupyterLab on Apache Spark 4.1 (Python 3, Java 21)
# Matches the Spark 4.1 runtime used by the rest of the stack so notebooks
# can execute pyspark 4.1 features (SDP, VARIANT, RTM, etc.) in local mode.
FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages \
      jupyterlab==4.3.0 \
      ipykernel==6.29.5 \
      pyspark==4.1.0 \
      pandas==2.2.3 \
      pyarrow==18.1.0 \
      matplotlib==3.9.2

ENV JUPYTER_ENABLE_LAB=yes \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /home/jovyan
EXPOSE 8889

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8889", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=", \
     "--ServerApp.password=", \
     "--ServerApp.disable_check_xsrf=True"]
