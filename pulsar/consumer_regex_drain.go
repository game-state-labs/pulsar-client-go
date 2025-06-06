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

func (c *regexConsumer) EnterDrainMode() error {
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()

	var errMsg string
	if c.closed() {
		return errors.New("regex consumer already closed")
	}

	for topic, consumer := range c.consumers {
		if consumerWithDrain, ok := consumer.(interface {
			EnterDrainMode() error
		}); ok {
			if err := consumerWithDrain.EnterDrainMode(); err != nil {
				errMsg += fmt.Sprintf("topic %s: %s; ", topic, err.Error())
			}
		} else {
			errMsg += fmt.Sprintf("topic %s: consumer does not support drain mode; ", topic)
		}
	}

	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func (c *regexConsumer) ExitDrainMode() error {
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()

	var errMsg string
	if c.closed() {
		return errors.New("regex consumer already closed")
	}

	for topic, consumer := range c.consumers {
		if consumerWithDrain, ok := consumer.(interface {
			ExitDrainMode() error
		}); ok {
			if err := consumerWithDrain.ExitDrainMode(); err != nil {
				errMsg += fmt.Sprintf("topic %s: %s; ", topic, err.Error())
			}
		} else {
			errMsg += fmt.Sprintf("topic %s: consumer does not support drain mode; ", topic)
		}
	}

	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}
