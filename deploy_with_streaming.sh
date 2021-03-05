DATAFLOW_PROJECT="evocative-nexus-302913"
BQ_PROJECT="evocative-nexus-302913"
BQ_DATASET="my_dataset"
PUBSUB_PROJECT="evocative-nexus-302913"
PUBSUB_SUBSCRIBTION="my_subscription"
REGION="us-central1"
BUCKET="my_first_project"
SA_FILE="$PWD/sa_key.json"
SA_ACCOUNT="dataflow-pipeline@evocative-nexus-302913.iam.gserviceaccount.com"
JOB_NAME="dataflow-bi"
MAX_WORKERS=1
WORKER_MACHINE_TYPE="n1-standard-2"

TEMP_LOCATION="gs://$BUCKET/tmp/"
STAGING_LOCATION="gs://$BUCKET/staging/"

ARGS="--runner=$1 
--tempLocation=$TEMP_LOCATION 
--stagingLocation=$STAGING_LOCATION 
--region=$REGION 
--enableStreamingEngine 
--numWorkers=1
--jobName=$JOB_NAME
--usePublicIps=false
--maxNumWorkers=$MAX_WORKERS
--autoscalingAlgorithm=THROUGHPUT_BASED
--project=$DATAFLOW_PROJECT 
--BQProject=$BQ_PROJECT 
--BQDataset=$BQ_DATASET 
--pubSubProject=$PUBSUB_PROJECT 
--subscription=$PUBSUB_SUBSCRIBTION
--bucket=$BUCKET
--serviceAccount=$SA_ACCOUNT
--streaming=true
--workerMachineType=$WORKER_MACHINE_TYPE"

echo $ARGS

if [ ! -f "$SA_FILE" ]; then
    echo "$SA_FILE does not exist."
    exit
fi

export GOOGLE_APPLICATION_CREDENTIALS=$SA_FILE
echo $GOOGLE_APPLICATION_CREDENTIALS

./gradlew clean run -Pargs="$ARGS"

unset GOOGLE_APPLICATION_CREDENTIALS