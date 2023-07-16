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

func TestQueryExecutorWorkflow(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (s *UnitTestSuite) TestQueryExecutorWorkflow() {
	var ac *QueryExecutorActivities
	s.env.RegisterActivity(ac.SpawnNodes)
	s.env.RegisterActivity(ac.GetKeeperInfo)

	s.env.ExecuteWorkflow(QueryExecutor, QueryExecutorInput{
		NumNodes: 3,
		Query:    "select * from urlCluster('{cluster}', 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv', 'CSVWithNames') LIMIT 5",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "workflow errored")

	var result QueryExecutorOutput
	s.NoError(s.env.GetWorkflowResult(&result))
	fmt.Printf("Workflow result: %+v\n", result)
}
