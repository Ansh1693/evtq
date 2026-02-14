package queue

import (
	lambdaruntime "github.com/Ansh1693/evtq/internal/lambda"
	"github.com/Ansh1693/evtq/internal/service"
	"github.com/Ansh1693/evtq/internal/store"
)

// Service alias provides queue-layer package path while preserving behavior.
type Service = service.Service

// Input aliases.
type CreateQueueInput = service.CreateQueueInput
type SendMessageInput = service.SendMessageInput
type ReceiveMessagesInput = service.ReceiveMessagesInput
type SendMessageBatchEntry = service.SendMessageBatchEntry
type DeleteMessageBatchEntry = service.DeleteMessageBatchEntry
type CreateTriggerInput = service.CreateTriggerInput
type UpdateTriggerInput = service.UpdateTriggerInput
type CreateFunctionInput = service.CreateFunctionInput
type UpdateFunctionInput = service.UpdateFunctionInput

// Error aliases used by API validation mapping.
var (
	ErrBodyTooLarge                  = service.ErrBodyTooLarge
	ErrInvalidDelay                  = service.ErrInvalidDelay
	ErrInvalidVisibility             = service.ErrInvalidVisibility
	ErrInvalidMaxMessages            = service.ErrInvalidMaxMessages
	ErrInvalidWaitTime               = service.ErrInvalidWaitTime
	ErrInvalidRetention              = service.ErrInvalidRetention
	ErrQueueNameRequired             = service.ErrQueueNameRequired
	ErrMessageBodyRequired           = service.ErrMessageBodyRequired
	ErrBatchTooLarge                 = service.ErrBatchTooLarge
	ErrBatchEmpty                    = service.ErrBatchEmpty
	ErrGroupIDRequired               = service.ErrGroupIDRequired
	ErrDedupIDRequired               = service.ErrDedupIDRequired
	ErrQueueNameNotFIFO              = service.ErrQueueNameNotFIFO
	ErrInvalidQueueType              = service.ErrInvalidQueueType
	ErrContentBasedDedupRequiresFIFO = service.ErrContentBasedDedupRequiresFIFO
	ErrInvalidTriggerTargetType      = service.ErrInvalidTriggerTargetType
	ErrInvalidTargetURL              = service.ErrInvalidTargetURL
	ErrInvalidLambdaFunctionName     = service.ErrInvalidLambdaFunctionName
	ErrInvalidTriggerBatchSize       = service.ErrInvalidTriggerBatchSize
	ErrInvalidTriggerBatchWindow     = service.ErrInvalidTriggerBatchWindow
	ErrInvalidTriggerConcurrency     = service.ErrInvalidTriggerConcurrency
	ErrInvalidVisibilityOverride     = service.ErrInvalidVisibilityOverride
	ErrInvalidFIFOGroupConcurrency   = service.ErrInvalidFIFOGroupConcurrency
	ErrInvalidPollerRange            = service.ErrInvalidPollerRange
	ErrInvalidFailureThreshold       = service.ErrInvalidFailureThreshold
	ErrInvalidFunctionName           = service.ErrInvalidFunctionName
	ErrInvalidFunctionRuntime        = service.ErrInvalidFunctionRuntime
	ErrInvalidFunctionHandler        = service.ErrInvalidFunctionHandler
	ErrInvalidFunctionTimeout        = service.ErrInvalidFunctionTimeout
	ErrInvalidFunctionMemory         = service.ErrInvalidFunctionMemory
	ErrInvalidFunctionCodePath       = service.ErrInvalidFunctionCodePath
	ErrInvalidFunctionStrategy       = service.ErrInvalidFunctionStrategy
	ErrInvalidWarmPoolSize           = service.ErrInvalidWarmPoolSize
)

func New(s *store.Store, lambdaRunner lambdaruntime.Invoker) *service.Service {
	return service.New(s, lambdaRunner)
}
