# Enhancing Consumer Drain Control

## Introduction

This fork of the official Apache Pulsar Go client introduces enhanced capabilities for controlling consumer message flow, specifically by adding a "drain mode." This mode allows applications to temporarily stop the ingestion of new messages from the Pulsar broker while ensuring that all messages already buffered within the client (both in the application-facing channel and the client's internal queues) can be processed and acknowledged.

#### Rationale for Drain Mode

The standard Pulsar Go client's permit-based flow control is designed for continuous consumption. When an application needs to perform certain operations like dynamic configuration changes, maintenance, graceful shutdowns while scaling downespecially in horizontally scaled environments like Kubernetes), it's often desirable to:

1. Stop accepting _new_ messages for a specific consumer or all consumers on a client.
2. Process and acknowledge all messages that have already been delivered and buffered by the client.
3. Resume normal consumption after necessary operations are completed or cleanly close the consumer.

The standard client behavior can lead to new messages being fetched as buffered ones are acknowledged due to permit renewal. This fork addresses this by providing explicit control to pause new permit requests during such draining operations.

#### Benefits of this Enhanced Drain Control

1. **Precise Inflow Stoppage**: Guarantees that no new messages are pulled from the broker while the consumer is in "drain mode," even as buffered messages are acknowledged.
2. **Complete Processing of Buffered Messages**: Facilitates the full processing of messages already residing in the application's `MessageChannel` and the client's internal `queueCh`.
3. **Minimized Operational Redeliveries**: By cleanly processing buffered messages before state changes (dynamic reconfiguration, shutdowns), it reduces the chances of messages being NACKed or becoming unacknowledged due to the operation itself, thereby minimizing operationally-induced redeliveries. This supports overall message processing order for a given key.
4. **`Key_Shared` Stickiness Support**: Aims to maintain `Key_Shared` subscription stickiness during the drain period by keeping the underlying connection to the broker alive, even though new message flow is paused. This is particularly important for ensuring keys are not prematurely reassigned during short operational pauses. [Not sure if this is necessary. Will need to test and validate this behavior.]
5. **Predictable Shutdowns/Updates**: Enables more deterministic behavior during deployments, scaling events, or rule updates in distributed environments.
