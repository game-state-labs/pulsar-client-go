# Enhancing Consumer Drain Control

## Introduction

This fork of the official Apache Pulsar Go client introduces enhanced capabilities for controlling consumer message flow, specifically by adding a "drain mode." This mode is designed to help applications manage message consumption more precisely during planned operational scenarios such as dynamic configuration updates for message processing or graceful service shutdowns, especially in horizontally scaled environments.

### Context: Pulsar's Message Ordering and Redelivery

Before detailing the fork's changes, it's important to briefly go over some of Pulsar's baseline message ordering and redelivery characteristics:

- **Pulsar's Ordering Guarantees**:

  - **Exclusive/Failover Subscriptions**: For a non-partitioned topic or a single partition of a topic, Pulsar guarantees that messages are delivered to the consumer in the same order they were successfully published by the producer. For partitioned topics, the ordering is maintained per partition.
  - **`Key_Shared` Subscriptions**: This subscription type allows multiple consumers to subscribe to a topic (or its partitions) while ensuring that all messages with the _same message key_ are routed to the _same consumer_. This inherently provides ordered delivery for the sequence of messages associated with a specific key, processed by that designated consumer. Improvements to the `AUTO_SPLIT` mode for `Key_Shared` subscriptions aim to better preserve the order of message delivery for a given key, even when consumers join or leave the group, minimizing disruptions.

- **Message Redelivery in Pulsar** Redelivery can occur in several scenarios:

  - **Unacknowledged Messages**: If a consumer receives a message but fails to acknowledge it before a configured `ackTimeout` (e.g., due to a consumer crash, prolonged processing, or lost acknowledgment), the broker will make the message available again for delivery, typically to the same consumer if still active, or another available consumer in the subscription.
  - **Negative Acknowledgements (`Nack`)**: If a consumer explicitly NACKs a message (e.g., due to a temporary inability to process it), the message is scheduled for redelivery by the broker after a configured delay.
  - **Consumer Seek Operations**: If a consumer uses the `Seek()` API to reset its subscription cursor to an earlier message ID or timestamp, messages from that point onwards will be delivered (or redelivered if they were part of the original stream).

- **Ordering Implications on Redelivery**:
  - When Pulsar redelivers an individual message, it sends that specific message again.
  - **Crucially, a redelivered message might arrive out of its original sequence relative to _newly published messages_ for that same key, or relative to other messages in the original sequence that were not redelivered.**

### Purpose and Benefits of This Fork's Drain Mode

The standard Pulsar Go client's permit-based flow control is optimized for continuous message consumption. During planned operational events (like dynamic configuration updates for message processing, or performing a graceful shutdown of a service instance), simply stopping the application from pulling messages can lead to a backlog in client buffers. These buffered messages if not processed in a timely manner may become unacknowledged, leading to them being redelivered by the broker.

This fork introduces an explicit **"drain mode"** to address these scenarios by providing precise control over new message inflow from the broker.

**How the Draining Mechanism Relates and Its Benefits**:

- **Preventing Operational-Induced Redeliveries**: The primary benefit of the drain mode is to **minimize message redeliveries that are a direct consequence of the operational procedure itself** (e.g., dynamic configuration updates, graceful shutdowns, etc.). It does not solve all redelivery scenarios. During such operations, _without controlled draining_ messages residing in client buffers (both the application's `MessageChannel` and the client's internal `queueCh`) might not be fully processed and ACKed, when a consumer is stopped or its processing logic changes mid-flight. These become unacknowledged from the broker's perspective and will be redelivered. _With this fork's `EnterDrainMode()`_:
  - The application explicitly command the client to stop requesting new message permits from the broker, even if buffered messages are being acknowledged.
  - This allows the application to safely process and acknowledge every message that has already been fetched by the client and is residing in its buffers.
  - This significantly reduces the pool of messages that would otherwise become unacknowledged (and thus redelivered) due to the shutdown or rule change itself.
- **Supporting Overall Orderliness for Keyed Messages During Operations**:
  - By minimizing these operational-induced redeliveries, the drain mode helps maintain the intended processing sequence for messages of a given key _during these planned events_. The main source of potential out-of-order redeliveries then shifts primarily to genuine application failures like NACKs or crashes before ACKs, which **must be handled by the application's idempotent processing logic**.

It's important to understand that this drain mode **does not alter Pulsar's core broker-side behavior for redelivery or ordering**. The fork provides a client-side tool to give the application more control to avoid _causing_ a large set of messages to require redelivery during planned maintenance, updates, or shutdowns.

Beyond addressing redelivery concerns, drain mode provides some operational advantages:

- **Clean Operational Transitions**: Facilitates smoother configuration updates and graceful shutdowns by ensuring all fetched messages are properly processed before the operation transitions.
- **Resource Management**: Prevents accumulation of unprocessed messages in client buffers during planned pauses, reducing memory pressure and potential message timeout issues.
- **Key Assignment Stability**: For `Key_Shared` subscriptions, helps maintain key-to-consumer assignments during brief operational windows by keeping connections active while controlling message flow. (The efficacy of this for very long pauses depends on broker configurations regarding inactive-but-connected consumers).
- **Horizontally Scaled Environments**: Supports more predictable behavior and cleaner state transitions during deployments, scaling events, or rule updates, particularly valuable in containerized or auto-scaled deployments.

The need for **idempotent message processing logic in the application remains absolutely crucial** for robustly handling all types of redelivery scenarios that can occur in a distributed messaging system. This "drain mode" assists in making planned operational procedures smoother and less prone to causing unnecessary redeliveries.

## API Usage

The drain mode feature is available on both the `Consumer` and `Reader` interfaces through two methods:

```go
// Consumer interface
func (c *consumer) EnterDrainMode() error
func (c *consumer) ExitDrainMode() error

// Reader interface (which wraps a Consumer)
func (r *reader) EnterDrainMode() error
func (r *reader) ExitDrainMode() error
```

### Example: Rule Change with Drain Mode

```go
// When processing logic/rules need to be updated:
func updateConsumerRules(consumer pulsar.Consumer) error {
    // 1. Enter drain mode to stop receiving new messages from broker
    if err := consumer.EnterDrainMode(); err != nil {
        return fmt.Errorf("failed to enter drain mode: %w", err)
    }

    // 2. Process all messages already in client buffers with current rules
    for {
        ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
        msg, err := consumer.Receive(ctx)
        cancel()

        if err != nil {
            // No more messages in buffer
            break
        }

        // Process with current rules
        processMessageWithCurrentRules(msg)
        consumer.Ack(msg)
    }

    // 3. Update processing rules
    updateRules()

    // 4. Resume normal message flow with new rules
    if err := consumer.ExitDrainMode(); err != nil {
        return fmt.Errorf("failed to exit drain mode: %w", err)
    }

    return nil
}
```

### Example: Graceful Shutdown with Drain Mode

```go
// For graceful application shutdown:
func performGracefulShutdown(consumer pulsar.Consumer, shutdownTimeout time.Duration) {
    // 1. Enter drain mode to stop receiving new messages from broker
    if err := consumer.EnterDrainMode(); err != nil {
        log.Printf("Warning: Failed to enter drain mode: %v", err)
    }

    // 2. Set a deadline for processing remaining messages
    ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
    defer cancel()

    // 3. Process remaining buffered messages until deadline
    for {
        receiveCtx, receiveCancel := context.WithTimeout(ctx, 500*time.Millisecond)
        msg, err := consumer.Receive(receiveCtx)
        receiveCancel()

        if err != nil {
            // Either no more messages or shutdown deadline reached
            break
        }

        // Process and acknowledge message
        processMessage(msg)
        consumer.Ack(msg)
    }

    // 4. Close the consumer
    consumer.Close()

    log.Println("Consumer shutdown gracefully")
}
```

## Metrics

To help monitor and track the usage of drain mode, the following metrics have been added:

- `pulsar_client_consumers_in_drain_mode`: A gauge that tracks the number of consumers currently in drain mode.
- `pulsar_client_drain_mode_entered`: A counter that increments every time a consumer enters drain mode.
- `pulsar_client_drain_mode_exited`: A counter that increments every time a consumer exits drain mode.

These metrics follow the same labeling patterns as other Pulsar client metrics, with labels added based on the configured `MetricsCardinality` (tenant, namespace, topic).
