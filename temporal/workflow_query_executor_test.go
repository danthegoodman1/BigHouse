package temporal

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/workflow"
	"testing"
	"time"

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
	s.env.SetTestTimeout(time.Second * 30)

	bigStmt := `
	SELECT count(), _file FROM s3Cluster('{cluster}', 'https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-*.csv.gz', 
	'CSVWithNames', 
	'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String', 
	'gzip') group by _file
	`
	bigStmt = bigStmt

	smallStmt := `SELECT count(), _file FROM url('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_{0,1}.parquet') GROUP BY _file LIMIT 10`
	smallStmt = smallStmt

	s.env.ExecuteWorkflow(QueryExecutor, QueryExecutorInput{
		NumNodes: 2,
		Query:    smallStmt,
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "workflow errored")

	var result QueryExecutorOutput
	s.NoError(s.env.GetWorkflowResult(&result))
	fmt.Printf("Workflow result: %+v\n", result)
}
