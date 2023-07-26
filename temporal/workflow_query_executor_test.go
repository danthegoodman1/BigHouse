package temporal

import (
	"fmt"
	"github.com/danthegoodman1/BigHouse/utils"
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
	s.env.SetTestTimeout(time.Minute * 100)

	bigStmt := `
	SELECT count(), _file FROM s3Cluster('{cluster}', 'https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-*.csv.gz', 
	'CSVWithNames', 
	'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String', 
	'gzip') group by _file
	`
	bigStmt = bigStmt

	smallStmt := `SELECT count(), _file FROM s3Cluster('{cluster}', 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_{0,1}.parquet') GROUP BY _file LIMIT 10`
	smallStmt = smallStmt

	// globStmt := `SELECT count(), _file FROM s3('https://ookla-open-data.s3.amazonaws.com/parquet/performance/type=*/year=*/quarter=*/*.parquet') GROUP BY _file`
	globStmt := `SELECT count() FROM s3('https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-*.csv.gz', 'CSVWithNames', 
	'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String', 
	'gzip')`
	globStmt = globStmt

	globStmtCluster := `SELECT count() FROM s3Cluster('{cluster}', 'https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-*.csv.gz', 'CSVWithNames', 
	'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String', 
	'gzip')`
	globStmtCluster = globStmtCluster

	globStmtClusterUrl := `SELECT count() FROM urlCluster('{cluster}', 'https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-{2009..2016}{01..12}.csv.gz', 'CSVWithNames', 
	'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String', 
	'gzip')`
	globStmtClusterUrl = globStmtClusterUrl

	shortStmt := `SELECT count() FROM s3('https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-201612.csv.gz', 'CSVWithNames', 
	'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String', 
	'gzip') limit 10`
	shortStmt = shortStmt

	s.env.ExecuteWorkflow(QueryExecutor, QueryExecutorInput{
		NumNodes: 6,
		Query:    "select 1",
		// Query:    "select sum(commits), repo_name from github_events_all group by repo_name settings max_parallel_replicas=1, allow_experimental_parallel_reading_from_replicas=0, prefer_localhost_replica=1, max_bytes_before_external_group_by=16384000000",
		// Query:       "select 1",
		KeeperHost:  "d891d64c621628.vm.test-bighouse-t-keeper.internal",
		Cluster:     utils.GenRandomAlpha(""),
		CPUKind:     "performance",
		Cores:       16,
		MemoryMB:    1024 * 32,
		DeleteNodes: false,
		InitQueries: []string{
			"ATTACH TABLE github_events UUID '127f4241-4a9b-4ecd-8a84-846b88069cb5' on cluster '{cluster}' \n(\n    `file_time` DateTime,\n    `event_type` Enum8('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),\n    `actor_login` LowCardinality(String),\n    `repo_name` LowCardinality(String),\n    `created_at` DateTime,\n    `updated_at` DateTime,\n    `action` Enum8('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),\n    `comment_id` UInt64,\n    `body` String,\n    `path` String,\n    `position` Int32,\n    `line` Int32,\n    `ref` LowCardinality(String),\n    `ref_type` Enum8('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4),\n    `creator_user_login` LowCardinality(String),\n    `number` UInt32,\n    `title` String,\n    `labels` Array(LowCardinality(String)),\n    `state` Enum8('none' = 0, 'open' = 1, 'closed' = 2),\n    `locked` UInt8,\n    `assignee` LowCardinality(String),\n    `assignees` Array(LowCardinality(String)),\n    `comments` UInt32,\n    `author_association` Enum8('NONE' = 0, 'CONTRIBUTOR' = 1, 'OWNER' = 2, 'COLLABORATOR' = 3, 'MEMBER' = 4, 'MANNEQUIN' = 5),\n    `closed_at` DateTime,\n    `merged_at` DateTime,\n    `merge_commit_sha` String,\n    `requested_reviewers` Array(LowCardinality(String)),\n    `requested_teams` Array(LowCardinality(String)),\n    `head_ref` LowCardinality(String),\n    `head_sha` String,\n    `base_ref` LowCardinality(String),\n    `base_sha` String,\n    `merged` UInt8,\n    `mergeable` UInt8,\n    `rebaseable` UInt8,\n    `mergeable_state` Enum8('unknown' = 0, 'dirty' = 1, 'clean' = 2, 'unstable' = 3, 'draft' = 4),\n    `merged_by` LowCardinality(String),\n    `review_comments` UInt32,\n    `maintainer_can_modify` UInt8,\n    `commits` UInt32,\n    `additions` UInt32,\n    `deletions` UInt32,\n    `changed_files` UInt32,\n    `diff_hunk` String,\n    `original_position` UInt32,\n    `commit_id` String,\n    `original_commit_id` String,\n    `push_size` UInt32,\n    `push_distinct_size` UInt32,\n    `member_login` LowCardinality(String),\n    `release_tag_name` String,\n    `release_name` String,\n    `review_state` Enum8('none' = 0, 'approved' = 1, 'changes_requested' = 2, 'commented' = 3, 'dismissed' = 4, 'pending' = 5)\n)\nENGINE = MergeTree\nORDER BY (event_type, repo_name, created_at)\nSETTINGS disk = disk(type = web, endpoint = 'http://clickhouse-public-datasets.s3.amazonaws.com/web/')",
			"SET allow_experimental_parallel_reading_from_replicas = 1, use_hedged_requests = 0, prefer_localhost_replica = 0, max_parallel_replicas = 10, cluster_for_parallel_replicas = '{cluster}', parallel_replicas_for_non_replicated_merge_tree = 1;)",
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "workflow errored")

	var result QueryExecutorOutput
	s.NoError(s.env.GetWorkflowResult(&result))
	fmt.Printf("Workflow result: %+v\n", result)
}

// SELECT
// name,
// elapsed_us,
// input_wait_elapsed_us,
// output_wait_elapsed_us
// FROM clusterAllReplicas('{cluster}', system.processors_profile_log)
// WHERE initial_query_id = '4398eec5-bb02-408f-8ea4-407cf9bbf2a5'
// ORDER BY name ASC;

// select * from system.zookeeper where path='/clickhouse/task_queue/'
