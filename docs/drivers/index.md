# Driver selection guide

Every driver is labelled with a maturity level and a maintenance tier (specification). Compare capabilities in the matrix.

| Driver | Page | Maturity · Tier | Choose it when |
|---|---|---|---|
| ActiveMQ over AMQP 1.0 (Artemis and Classic) | [activemq.md](activemq.md) | Experimental · 3 | You are on either ActiveMQ product. It is the default: the most feature-complete of the four ActiveMQ combinations, with a real negative settlement, a real attempt count, binary header values and the only topology surface any of them has (on Artemis). |
| ActiveMQ over STOMP 1.2 (Artemis and Classic) | [activemq-stomp.md](activemq-stomp.md) | Experimental · 3 | You are on **ActiveMQ Classic and need a broker-side selector** — the one thing the AMQP 1.0 arm cannot do — or STOMP is the only port you are allowed through. Not if you nack often: a STOMP `NACK` returns nothing on either broker, so a requeue is a subscription reset that returns *every* held delivery. |

The two ActiveMQ pages cross-reference each other and
[choosing between the four combinations](activemq.md#choosing-between-the-four-combinations)
compares all four (two products × two protocols) against criterion.
