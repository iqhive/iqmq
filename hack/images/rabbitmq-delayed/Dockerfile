# RabbitMQ with the delayed-message exchange plugin, for the A17 bake-off and
# the ScheduledPublish conditional claim. The plugin only supports the broker
# minor it was built for (4.2.0 -> 4.2.x), so this image tracks the newest
# RabbitMQ minor with a plugin release, not the main rabbitmq pin.
# Build: make image-rabbitmq-delayed
ARG RABBITMQ_TAG=4.2.9-management-alpine
FROM rabbitmq:${RABBITMQ_TAG}
ARG PLUGIN_VERSION=4.2.0
ARG PLUGIN_URL=https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v${PLUGIN_VERSION}/rabbitmq_delayed_message_exchange-${PLUGIN_VERSION}.ez
RUN wget -q -O /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-${PLUGIN_VERSION}.ez "${PLUGIN_URL}" \
 && rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange
