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
)

// enterInternalDrainMode puts the partition consumer into drain mode
func (pc *partitionConsumer) enterInternalDrainMode() error {
	if state := pc.getConsumerState(); state == consumerClosed || state == consumerClosing {
		pc.log.WithField("state", state).Error("Failed to enter drain mode: consumer closing or closed")
		return errors.New("consumer closing or closed")
	}

	wasAlreadyDraining := pc.isDrainingNoNewPermits.Swap(true)
	if wasAlreadyDraining {
		pc.log.Debug("Consumer was already in drain mode")
		return nil
	}

	if pc.metrics != nil {
		pc.metrics.ConsumersInDrainMode.Inc()
		pc.metrics.DrainModeEntered.Inc()
	}

	pc.log.Info("Consumer entered drain mode - no new messages will be fetched from broker")
	return nil
}

// exitInternalDrainMode takes the partition consumer out of drain mode
func (pc *partitionConsumer) exitInternalDrainMode() error {
	if state := pc.getConsumerState(); state == consumerClosed || state == consumerClosing {
		pc.log.WithField("state", state).Error("Failed to exit drain mode: consumer closing or closed")
		return errors.New("consumer closing or closed")
	}

	wasDraining := pc.isDrainingNoNewPermits.Swap(false)
	if !wasDraining {
		pc.log.Debug("Consumer was not in drain mode")
		return nil
	}

	if pc.metrics != nil {
		pc.metrics.ConsumersInDrainMode.Dec()
		pc.metrics.DrainModeExited.Inc()
	}

	// Trigger permit flow to resume message delivery if there are available permits
	pc.availablePermits.flowIfNeed()

	pc.log.Info("Consumer exited drain mode - normal message flow resumed")
	return nil
}
