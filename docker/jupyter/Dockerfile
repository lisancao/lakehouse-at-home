# JupyterLab on Apache Spark 4.1 (Python 3, Java 21)
# Matches the Spark 4.1 runtime used by the rest of the stack so notebooks
# can execute pyspark 4.1 features (SDP, VARIANT, RTM, etc.) in local mode.
FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-pip python3-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Use a venv to sidestep PEP 668 on newer Ubuntu base images (and to avoid
# clobbering the base image's system-site-packages). Pin pip first so the
# jupyterlab resolver has a modern version.
ENV VIRTUAL_ENV=/opt/jupyter-venv
RUN python3 -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install --no-cache-dir --upgrade "pip>=24.0" setuptools wheel
RUN pip install --no-cache-dir \
      jupyterlab==4.3.0 \
      ipykernel==6.29.5 \
      pyspark==4.1.0 \
      pandas==2.2.3 \
      pyarrow==18.1.0 \
      matplotlib==3.9.2 \
      "httpx<0.28"   # jupyterlab 4.3 uses httpx AsyncClient(proxies=...) removed in 0.28

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
