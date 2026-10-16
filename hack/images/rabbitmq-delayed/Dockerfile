# RabbitMQ with the delayed-message exchange plugin, for the A17 bake-off and
# the ScheduledPublish conditional claim. The plugin only supports the broker
# minor it was built for (4.2.0 -> 4.2.x), so this image tracks the newest
# RabbitMQ minor with a plugin release, not the main rabbitmq pin.
# Build: make image-rabbitmq-delayed
FROM rabbitmq:4.2.9-management-alpine@sha256:5606bedb254b7b5bcf890ad8d9237abb59f9bc0ee98d9e9b9f0c113f4f489b6c
RUN wget -q -O /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-4.2.0.ez \
      https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v4.2.0/rabbitmq_delayed_message_exchange-4.2.0.ez \
 && echo 'f168b2c09810cde3726961d31f38e3408e6a7fbff3929908d6f962061d8e70a1  /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-4.2.0.ez' | sha256sum -c - \
 && rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange
