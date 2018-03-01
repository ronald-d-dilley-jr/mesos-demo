docker run --rm --detach \
    --env RABBITMQ_DEFAULT_USER=${DEMO_RABBITMQ_DEFAULT_USER} \
    --env RABBITMQ_DEFAULT_PASS=${DEMO_RABBITMQ_DEFAULT_PASS} \
    --publish "5672:5672" --publish "15672:15672" \
    --name message_service \
    rabbitmq:3.7.3-management-alpine
