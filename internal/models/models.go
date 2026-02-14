package models

import (
	base "github.com/Ansh1693/evtq/internal"
)

// Type aliases keep behavior unchanged while exposing a dedicated models package.
type Queue = base.Queue
type Message = base.Message
type QueueStats = base.QueueStats
type BatchResultEntry = base.BatchResultEntry

type Trigger = base.Trigger
type TriggerWithQueue = base.TriggerWithQueue
type TriggerRecord = base.TriggerRecord
type TriggerPayload = base.TriggerPayload
type BatchItemFailure = base.BatchItemFailure
type TriggerInvocationResponse = base.TriggerInvocationResponse
type TriggerMetrics = base.TriggerMetrics

type Function = base.Function

const (
	TriggerTargetTypeWebhook = base.TriggerTargetTypeWebhook
	TriggerTargetTypeGRPC    = base.TriggerTargetTypeGRPC
	TriggerTargetTypeLocal   = base.TriggerTargetTypeLocal
	TriggerTargetTypeLambda  = base.TriggerTargetTypeLambda
)
