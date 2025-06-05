// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
)

func TestConsumerDrainMode(t *testing.T) {
	sLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	client, err := NewClient(ClientOptions{
		URL:    lookupURL,
		Logger: log.NewLoggerWithSlog(sLogger),
	})
	assert.Nil(t, err)
	defer client.Close()

	topicName := fmt.Sprintf("consumer-drain-mode-test-%v", time.Now().UnixNano())
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	queueSize := 4
	messageChanSize := 10
	totalBufferCapacity := queueSize + messageChanSize

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:             topicName,
		SubscriptionName:  "drain-mode-sub",
		Type:              Shared,
		ReceiverQueueSize: queueSize, // queueCh size is set to ReceiverQueueSize
		// Note: messageCh capacity is hardcoded to 10 in the client implementation
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// Phase 1: Produce initial messages
	initialMessages := 40
	for i := range initialMessages {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: fmt.Appendf(nil, "normal-msg-%d", i),
		})
		assert.Nil(t, err)
	}

	// Give some time for messages to be delivered to broker and initial permits to be received
	// This ensures the client buffers are filled
	time.Sleep(500 * time.Millisecond)

	// Phase 2: Enter drain mode before consuming any messages
	err = consumer.EnterDrainMode()
	assert.Nil(t, err)

	// Consume messages from the buffers while in drain mode
	// We expect to receive exactly totalBufferCapacity messages
	drainModeMessages := make([]string, 0, totalBufferCapacity)

	// Consume messages with a timeout to get all buffered messages
	drainTimeout := 5 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < drainTimeout {
		receiveCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		msg, err := consumer.Receive(receiveCtx)
		cancel()

		if err != nil {
			// No more messages to receive
			break
		}

		payload := string(msg.Payload())
		drainModeMessages = append(drainModeMessages, payload)
		// t.Logf("Received from buffer in drain mode (%d/%d expected): %s",
		// 	len(drainModeMessages), totalBufferCapacity, payload)
		consumer.Ack(msg)
	}

	// Verify we received exactly the expected number of messages
	// t.Logf("Received %d messages from buffer while in drain mode", len(drainModeMessages))
	assert.Equal(t, totalBufferCapacity, len(drainModeMessages),
		"Should receive exactly %d messages from buffer in drain mode", totalBufferCapacity)

	// Phase 3: Send new messages while in drain mode and verify they aren't received
	drainModeProducedCount := 20
	for i := range drainModeProducedCount {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: fmt.Appendf(nil, "drain-produced-msg-%d", i),
		})
		assert.Nil(t, err)
	}

	// Wait a bit to ensure messages have reached the broker
	time.Sleep(1 * time.Second)

	// Try to receive more messages - should time out as no new messages should be delivered in drain mode
	// t.Log("Verifying no new messages are received while in drain mode")
	receiveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = consumer.Receive(receiveCtx)
	cancel()

	// We should timeout since the broker should not deliver new messages in drain mode
	assert.NotNil(t, err, "Should timeout as no new messages should be delivered in drain mode")
	t.Logf("Successfully confirmed no new messages while in drain mode: %v", err)

	// Phase 4: Exit drain mode and verify new messages start flowing
	// t.Log("Phase 4: Exiting drain mode and verifying message flow resumes")
	err = consumer.ExitDrainMode()
	assert.Nil(t, err)

	// Now we should receive some of the previously produced messages
	// Let's verify we receive at least minExpected messages after exiting drain mode
	minExpected := initialMessages + drainModeProducedCount - totalBufferCapacity
	messagesAfterDrain := make([]string, 0, minExpected)

	// Give some time for permits to be received and messages to be delivered
	time.Sleep(1 * time.Second)

	// t.Logf("Expecting to receive at least %d messages after exiting drain mode", minExpected)
	for range minExpected {
		receiveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := consumer.Receive(receiveCtx)
		cancel()

		if err != nil {
			// t.Logf("Failed to receive message after drain mode (%d/%d): %v",
			// 	len(messagesAfterDrain), minExpected, err)
			break
		}

		payload := string(msg.Payload())
		messagesAfterDrain = append(messagesAfterDrain, payload)
		// t.Logf("Received after exiting drain mode (%d/%d): %s",
		// 	len(messagesAfterDrain), minExpected, payload)
		consumer.Ack(msg)
	}

	// Verify we received at least the minimum expected messages after exiting drain mode
	assert.GreaterOrEqual(t, len(messagesAfterDrain), minExpected,
		"Should receive at least %d messages after exiting drain mode", minExpected)
	// t.Logf("Received %d messages after exiting drain mode", len(messagesAfterDrain))

	// Final verification
	// t.Log("Test completed successfully, drain mode behavior verified")
}

func TestConsumerBufferDrainMode(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topicName := fmt.Sprintf("consumer-buffer-drain-test-%v", time.Now().UnixNano())
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	queueSize := 20
	messageChanSize := 10
	totalBufferCapacity := queueSize + messageChanSize

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:             topicName,
		SubscriptionName:  "buffer-drain-sub",
		Type:              Shared,
		ReceiverQueueSize: queueSize,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	messageCount := 40
	for i := range messageCount {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: fmt.Appendf(nil, "buffer-msg-%d", i),
		})
		assert.Nil(t, err)
	}

	// Give some time for messages to be delivered to client buffer
	time.Sleep(1 * time.Second)

	// Enter drain mode before receiving any messages
	err = consumer.EnterDrainMode()
	assert.Nil(t, err)

	// Now consume messages from the client buffer while in drain mode
	// We expect to receive exactly totalBufferCapacity messages
	messagesInBuffer := make([]string, 0, totalBufferCapacity)
	drainTimeout := 5 * time.Second
	startTime := time.Now()

	// Keep receiving messages until we get the expected count or timeout
	for len(messagesInBuffer) < totalBufferCapacity && time.Since(startTime) < drainTimeout {
		receiveCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		msg, err := consumer.Receive(receiveCtx)
		cancel()

		if err != nil {
			t.Logf("Received error after %d messages: %v", len(messagesInBuffer), err)
			break
		}

		payload := string(msg.Payload())
		messagesInBuffer = append(messagesInBuffer, payload)
		t.Logf("Received from buffer in drain mode (%d/%d expected): %s",
			len(messagesInBuffer), totalBufferCapacity, payload)
		consumer.Ack(msg)
	}

	// Verify we received exactly the expected number of messages
	assert.Equal(t, totalBufferCapacity, len(messagesInBuffer),
		"Should receive exactly %d messages from buffer in drain mode", totalBufferCapacity)

	// Try to receive more messages - should time out since we're in drain mode and buffer is emptied
	// t.Log("Verifying no more messages come through after buffer is emptied")
	receiveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = consumer.Receive(receiveCtx)
	cancel()
	assert.NotNil(t, err, "Should timeout as no new messages should be delivered in drain mode")

	// Produce more messages while in drain mode
	moreMessageCount := 15
	for i := range moreMessageCount {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: fmt.Appendf(nil, "after-drain-msg-%d", i),
		})
		assert.Nil(t, err)
	}

	// Verify we can't receive these messages while in drain mode
	receiveCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	_, err = consumer.Receive(receiveCtx2)
	cancel2()
	assert.NotNil(t, err, "Should timeout as no new messages should be delivered in drain mode")
	// t.Logf("Successfully confirmed no new messages while in drain mode: %v", err)

	// Now exit drain mode and verify we get more messages
	// t.Log("Exiting drain mode to verify normal message flow resumes")
	err = consumer.ExitDrainMode()
	assert.Nil(t, err)

	// Give some time for permits to be received and messages to be delivered
	time.Sleep(1 * time.Second)

	// Try to receive messages after exiting drain mode
	// We expect to receive at least some minimum number of the messages we sent during drain mode
	minExpected := messageCount + moreMessageCount - totalBufferCapacity
	messagesAfterDrain := make([]string, 0, minExpected)

	for range minExpected {
		receiveCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		msg, err := consumer.Receive(receiveCtx)
		cancel()

		if err != nil {
			// t.Logf("Error receiving message after drain mode: %v", err)
			break
		}

		payload := string(msg.Payload())
		messagesAfterDrain = append(messagesAfterDrain, payload)
		// t.Logf("Received after drain mode (%d/%d minimum): %s",
		// 	len(messagesAfterDrain), minExpected, payload)
		consumer.Ack(msg)

		// Once we've received the minimum required count, we can stop
		if len(messagesAfterDrain) >= minExpected {
			break
		}
	}

	// Verify we received at least the minimum expected messages after exiting drain mode
	assert.GreaterOrEqual(t, len(messagesAfterDrain), minExpected,
		"Should receive at least %d messages after exiting drain mode", minExpected)
	t.Logf("Received %d messages after exiting drain mode", len(messagesAfterDrain))

	// t.Log("Buffer drain test completed successfully")
}
