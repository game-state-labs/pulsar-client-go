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

	pkgerrors "github.com/pkg/errors"
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
	var errMsg string
	for _, consumer := range c.partitionConsumers() {
		if err := consumer.enterInternalDrainMode(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s; ", consumer.topic, c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func (c *consumer) ExitDrainMode() error {
	var errMsg string
	for _, consumer := range c.partitionConsumers() {
		if err := consumer.exitInternalDrainMode(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s; ", consumer.topic, c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func (c *multiTopicConsumer) EnterDrainMode() error {
	var errs error
	for t, consumer := range c.consumers {
		drainable, ok := consumer.(ConsumerWithDrainMode)
		if !ok {
			errs = pkgerrors.Errorf("consumer for topic=%s subscription=%s does not support drain mode",
				t, c.Subscription())
			continue
		}
		if err := drainable.EnterDrainMode(); err != nil {
			msg := fmt.Sprintf("unable to enter drain mode for topic=%s subscription=%s",
				t, c.Subscription())
			errs = pkgerrors.Wrap(err, msg)
		}
	}
	return errs
}

func (c *multiTopicConsumer) ExitDrainMode() error {
	var errs error
	for t, consumer := range c.consumers {
		drainable, ok := consumer.(ConsumerWithDrainMode)
		if !ok {
			errs = pkgerrors.Errorf("consumer for topic=%s subscription=%s does not support drain mode",
				t, c.Subscription())
			continue
		}
		if err := drainable.ExitDrainMode(); err != nil {
			msg := fmt.Sprintf("unable to exit drain mode for topic=%s subscription=%s",
				t, c.Subscription())
			errs = pkgerrors.Wrap(err, msg)
		}
	}
	return errs
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
