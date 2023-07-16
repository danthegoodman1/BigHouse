package temporal

import (
	"context"

	"github.com/rs/zerolog"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

type (
	// contextKey is an unexported type used as key for items stored in the
	// Context object
	contextKey struct{}

	// propagator implements the custom context propagator
	propagator struct{}

	// ContextValues is a struct holding values
	ContextValues map[string]string
)

// PropagateKey is the key used to store the value in the Context object
var PropagateKey = contextKey{}

// propagationKey is the key used by the propagator to pass values through the
// Temporal server headers
const propagationKey = "tangia-log"

// NewContextPropagator returns a context propagator that propagates a set of
// string key-value pairs across a workflow
func NewContextPropagator() workflow.ContextPropagator {
	return &propagator{}
}

func AddContext(ctx context.Context, k, v string) context.Context {
	kv, ok := ctx.Value(PropagateKey).(ContextValues)
	if kv == nil || !ok {
		kv = ContextValues{}
	}
	kv[k] = v
	return context.WithValue(ctx, PropagateKey, kv)
}

// Inject injects values from context into headers for propagation
func (s *propagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value(PropagateKey)
	if value == nil {
		return nil
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		return err
	}
	writer.Set(propagationKey, payload)
	return nil
}

// InjectFromWorkflow injects values from context into headers for propagation
func (s *propagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value(PropagateKey)
	kv, ok := value.(ContextValues)
	if !ok {
		kv = map[string]string{}
	}
	kv["WorkflowType"] = workflow.GetInfo(ctx).WorkflowType.Name
	kv["WorkflowID"] = workflow.GetInfo(ctx).WorkflowExecution.ID
	kv["RunID"] = workflow.GetInfo(ctx).WorkflowExecution.RunID
	payload, err := converter.GetDefaultDataConverter().ToPayload(kv)
	if err != nil {
		return err
	}
	writer.Set(propagationKey, payload)
	return nil
}

// Extract extracts values from headers and puts them into context
func (s *propagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if value, ok := reader.Get(propagationKey); ok {
		var values ContextValues
		if err := converter.GetDefaultDataConverter().FromPayload(value, &values); err != nil {
			return ctx, err
		}
		ctx = context.WithValue(ctx, PropagateKey, values)
		logger := zerolog.Ctx(ctx).With()
		for k, v := range values {
			logger = logger.Str(k, v)
		}
		ctx = logger.Logger().WithContext(ctx)
	}
	return ctx, nil
}

// ExtractToWorkflow extracts values from headers and puts them into context
func (s *propagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	if value, ok := reader.Get(propagationKey); ok {
		var values ContextValues
		if err := converter.GetDefaultDataConverter().FromPayload(value, &values); err != nil {
			return ctx, err
		}
		ctx = workflow.WithValue(ctx, PropagateKey, values)
		// this relies on the GetLogger() method as zerolog doesn't expect Temporal's own Context type
	}
	return ctx, nil
}
