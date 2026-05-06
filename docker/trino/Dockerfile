FROM trinodb/trino:442

# Copy config templates (with __PLACEHOLDER__ tokens)
COPY docker/trino/etc/ /etc/trino/

# Entrypoint that substitutes env vars then starts Trino
COPY docker/trino/entrypoint.sh /usr/local/bin/entrypoint.sh

USER root
RUN chmod +x /usr/local/bin/entrypoint.sh && \
    sed -i -e 's/\r$//' /usr/local/bin/entrypoint.sh && \
    chown -R trino:trino /etc/trino
USER trino

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
