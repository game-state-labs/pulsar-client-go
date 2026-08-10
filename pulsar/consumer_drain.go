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
	"errors"
	"fmt"
)

// ConsumerWithDrainMode is a Consumer that additionally supports drain mode.
type ConsumerWithDrainMode interface {
	Consumer

	// EnterDrainMode stops sending permit flow requests to the broker, preventing
	// delivery of any new messages while allowing the consumer to process existing
	// messages in the client buffer.
	EnterDrainMode() error

	// ExitDrainMode resumes normal message flow by allowing the consumer to send
	// permit flow requests to the broker for new message delivery.
	ExitDrainMode() error
}

func (c *consumer) EnterDrainMode() error {
	var errs []error
	for _, consumer := range c.partitionConsumers() {
		if err := consumer.enterInternalDrainMode(); err != nil {
			errs = append(errs, fmt.Errorf("topic %s, subscription %s: %w", consumer.topic, c.Subscription(), err))
		}
	}
	return errors.Join(errs...)
}

func (c *consumer) ExitDrainMode() error {
	var errs []error
	for _, consumer := range c.partitionConsumers() {
		if err := consumer.exitInternalDrainMode(); err != nil {
			errs = append(errs, fmt.Errorf("topic %s, subscription %s: %w", consumer.topic, c.Subscription(), err))
		}
	}
	return errors.Join(errs...)
}

func (c *multiTopicConsumer) EnterDrainMode() error {
	var errs []error
	for t, consumer := range c.consumers {
		drainable, ok := consumer.(ConsumerWithDrainMode)
		if !ok {
			errs = append(errs, fmt.Errorf("consumer for topic=%s subscription=%s does not support drain mode",
				t, c.Subscription()))
			continue
		}
		if err := drainable.EnterDrainMode(); err != nil {
			errs = append(errs, fmt.Errorf("unable to enter drain mode for topic=%s subscription=%s: %w",
				t, c.Subscription(), err))
		}
	}
	return errors.Join(errs...)
}

func (c *multiTopicConsumer) ExitDrainMode() error {
	var errs []error
	for t, consumer := range c.consumers {
		drainable, ok := consumer.(ConsumerWithDrainMode)
		if !ok {
			errs = append(errs, fmt.Errorf("consumer for topic=%s subscription=%s does not support drain mode",
				t, c.Subscription()))
			continue
		}
		if err := drainable.ExitDrainMode(); err != nil {
			errs = append(errs, fmt.Errorf("unable to exit drain mode for topic=%s subscription=%s: %w",
				t, c.Subscription(), err))
		}
	}
	return errors.Join(errs...)
}

func (z *zeroQueueConsumer) EnterDrainMode() error {
	z.Lock()
	defer z.Unlock()

	return z.pc.enterInternalDrainMode()
}

func (z *zeroQueueConsumer) ExitDrainMode() error {
	z.Lock()
	defer z.Unlock()

	return z.pc.exitInternalDrainMode()
}
