docker inspect \
    -f "{{ .NetworkSettings.Networks.bridge.Gateway }}" \
    message_service
