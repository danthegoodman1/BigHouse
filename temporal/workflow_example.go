package temporal

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/workflow"
	"time"
)

var (
	WorkflowExamplePrefix = "wf_sample"
)

type (
	WorkflowExampleActivities struct{}

	WorkflowExampleInput struct {
		Num int
	}
	WorkflowExampleOutput struct {
		DoubleNum int
	}
)

func WorkflowExample(ctx workflow.Context, input WorkflowExampleInput) (*WorkflowExampleOutput, error) {
	logger := GetLogger(ctx)
	logger.Info().Msg("starting example workflow")

	var ac *WorkflowExampleActivities
	out, err := execActivityIO(ctx, ac.DoubleNumber, input.Num, time.Second*5)
	if err != nil {
		return nil, fmt.Errorf("error in DoubleNumber: %w", err)
	}
	return &WorkflowExampleOutput{DoubleNum: out}, nil
}

func (ac *WorkflowExampleActivities) DoubleNumber(ctx context.Context, input int) (int, error) {
	return input * 2, nil
}
