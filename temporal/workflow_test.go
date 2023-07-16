package temporal

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/workflow"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *UnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{NewContextPropagator()})
}

func (s *UnitTestSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func TestWorkflowExample(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (s *UnitTestSuite) TestWorkflowExample() {
	var ac *WorkflowExampleActivities
	s.env.RegisterActivity(ac.DoubleNumber)

	s.env.ExecuteWorkflow(WorkflowExample, WorkflowExampleInput{Num: 2})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "workflow errored")

	var result WorkflowExampleOutput
	s.NoError(s.env.GetWorkflowResult(&result))
	fmt.Printf("Got result %d * 2 = %d\n", 2, result.DoubleNum)
}
