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

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topicName := "drain-mode-example"

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             topicName,
		SubscriptionName:  "drain-example-sub",
		Type:              pulsar.Shared,
		ReceiverQueueSize: 20, // Set a reasonable queue size
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topicName,
		DisableBatching: true, // For more predictable message delivery in this example
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	ctx := context.Background()

	var (
		wg                sync.WaitGroup
		producerWg        sync.WaitGroup
		inDrainMode       = false
		drainModeStarted  = make(chan struct{})
		drainModeComplete = make(chan struct{})
		processingDone    = make(chan struct{})
		messagesMutex     sync.Mutex
		messagesInBuffer  = make(map[string]bool)
	)

	// Start consumer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		drainModeProcessedCount := 0

		for {
			// After processing some messages, enter drain mode
			if count == 5 && !inDrainMode {
				inDrainMode = true

				// Record current state of messages that should be in buffer
				messagesMutex.Lock()
				fmt.Printf("Entering drain mode - Messages that should be in client buffer when entering drain mode: %d\n", len(messagesInBuffer))
				messagesMutex.Unlock()

				if err := consumer.EnterDrainMode(); err != nil {
					log.Printf("Error entering drain mode: %v", err)
				}

				close(drainModeStarted)
			}

			msgCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			msg, err := consumer.Receive(msgCtx)
			cancel()

			if err != nil {
				if inDrainMode {
					// In drain mode, a timeout indicates we've emptied the client buffer
					messagesMutex.Lock()
					remainingBuffered := len(messagesInBuffer)
					messagesMutex.Unlock()

					if remainingBuffered > 0 {
						fmt.Printf("Drain mode confirmed: Timeout receiving messages while %d are available at broker\n", remainingBuffered)
					}

					// Only exit drain mode after processing some messages in drain mode
					if drainModeProcessedCount > 0 && count >= 10 {
						fmt.Printf("Processed %d messages while in drain mode\n", drainModeProcessedCount)

						messagesMutex.Lock()
						pendingMessages := len(messagesInBuffer)
						messagesMutex.Unlock()

						fmt.Printf("Exiting drain mode - Messages still pending at broker (not delivered due to drain mode): %d\n", pendingMessages)
						if err := consumer.ExitDrainMode(); err != nil {
							log.Printf("Error exiting drain mode: %v", err)
						}

						// Signal that we've exited drain mode
						close(drainModeComplete)
						inDrainMode = false
					}
				} else {
					fmt.Printf("Receive timed out in normal mode\n")
				}

				// If we've processed enough messages total, exit the consumer
				if count >= 25 {
					fmt.Println("Finished processing all messages, exiting consumer")
					close(processingDone)
					return
				}

				time.Sleep(200 * time.Millisecond)
				continue
			}

			// Process the received message
			payload := string(msg.Payload())
			count++

			// Remove this message from our tracking of buffered messages
			messagesMutex.Lock()
			delete(messagesInBuffer, payload)
			messagesMutex.Unlock()

			// Track messages processed while in drain mode separately
			if inDrainMode {
				drainModeProcessedCount++
				fmt.Printf("DRAIN MODE - Received from buffer: '%s' (buffer message #%d)\n", payload, drainModeProcessedCount)
			} else {
				fmt.Printf("NORMAL MODE - Received: '%s' (total #%d)\n", payload, count)
			}

			// Acknowledge message
			consumer.Ack(msg)

			// Simulate message processing time
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Producer goroutine - sends messages in batches to demonstrate drain mode
	producerWg.Add(1)
	go func() {
		defer producerWg.Done()

		// First batch - send a large batch of messages to fill the client buffer
		msgCount := 20 // Match our ReceiverQueueSize
		for i := range msgCount {
			msgPayload := fmt.Sprintf("message-batch1-%d", i)

			// Track this message as expected to be in client buffer
			messagesMutex.Lock()
			messagesInBuffer[msgPayload] = true
			messagesMutex.Unlock()

			if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(msgPayload),
			}); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Published: %s\n", msgPayload)

			// Small delay between messages
			time.Sleep(50 * time.Millisecond)
		}

		// Wait for drain mode to start
		<-drainModeStarted
		fmt.Println("Consumer entered drain mode")

		time.Sleep(1 * time.Second)

		// Send more messages while consumer is in drain mode
		drainBatchSize := 15

		for i := range drainBatchSize {
			msgPayload := fmt.Sprintf("drain-mode-msg-%d", i)

			// Track messages sent during drain mode
			messagesMutex.Lock()
			messagesInBuffer[msgPayload] = true
			messagesMutex.Unlock()

			if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(msgPayload),
			}); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Published while in drain mode: %s\n", msgPayload)

			time.Sleep(100 * time.Millisecond)
		}

		// Wait for consumer to exit drain mode
		<-drainModeComplete
		fmt.Println("Consumer exited drain mode")

		// Send a final batch of messages after drain mode
		finalBatchSize := 5
		for i := range finalBatchSize {
			msgPayload := fmt.Sprintf("after-drain-msg-%d", i)

			messagesMutex.Lock()
			messagesInBuffer[msgPayload] = true
			messagesMutex.Unlock()

			if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(msgPayload),
			}); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Published after drain mode: %s\n", msgPayload)

			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case <-processingDone:
		messagesMutex.Lock()
		undeliveredCount := len(messagesInBuffer)
		messagesMutex.Unlock()

		if undeliveredCount > 0 {
			fmt.Printf("Warning: %d messages were produced but not consumed\n", undeliveredCount)
		} else {
			fmt.Println("Successfully processed all produced messages!")
		}

	case <-time.After(30 * time.Second):
		fmt.Println("Example timed out")

		messagesMutex.Lock()
		fmt.Printf("There are %d messages that were produced but not consumed\n", len(messagesInBuffer))
		messagesMutex.Unlock()
	}

	producerWg.Wait()
	wg.Wait()

}
