# Ingestion pollers: one image, three services (commodities / fx / weather).
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LAST_KNOWN_STATE_PATH=/var/lib/gdep/last_known.json

WORKDIR /opt/app

COPY docker/requirements-pollers.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion ./ingestion

# Unprivileged, with one writable dir for the last-known-price state.
RUN useradd --create-home --uid 10001 gdep \
    && mkdir -p /var/lib/gdep \
    && chown -R gdep:gdep /var/lib/gdep /opt/app
USER gdep

ENTRYPOINT ["python", "-m", "ingestion.run_poller"]
CMD ["--source", "commodities"]
