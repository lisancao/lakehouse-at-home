# Spark 4.1 Connect server image — extends the official apache/spark image
# with the Python deps the Connect-native spark-pipelines CLI requires at
# runtime but the upstream image doesn't ship.
#
# Why this exists:
#   * apache/spark:4.1.0-...-python3-r-ubuntu ships pandas==2.1.4 and no
#     pyarrow / pyyaml / grpcio / zstandard. The Connect-native pipelines CLI
#     fails to import without all of them. (Likely regression from
#     SPARK-37554's image-deps work.)
#   * Pip-installing at container start adds 30s to boot and a network
#     dependency. Bake them.
FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

USER root

RUN pip install --no-cache-dir \
    'pandas>=2.2' \
    'pyarrow>=15' \
    'grpcio>=1.62' \
    'grpcio-status>=1.62' \
    googleapis-common-protos \
    protobuf \
    'zstandard>=0.25' \
    pyyaml

# Pre-create the SDP storage path so the spec's file:///opt/sdp-storage
# resolves on the volume mount without a postStart hook.
RUN mkdir -p /opt/sdp-storage \
    && chown 185:0 /opt/sdp-storage
