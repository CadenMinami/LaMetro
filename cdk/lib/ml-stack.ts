import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import * as fs from 'fs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';

export interface MLStackProps extends cdk.StackProps {
  routeAggregatesTable: dynamodb.ITable;
  weatherCacheTable: dynamodb.ITable;
  archiveBucket: s3.IBucket;
  routePredictionsTable: dynamodb.ITable;
}

/**
 * Phase 7a — ML data foundation.
 *
 * Houses the durable feature-store writer + the Glue table that makes it
 * Athena-queryable. Later phases (7b training pipeline, 7c inference serving)
 * extend this same stack.
 */
export class MLStack extends cdk.Stack {
  public readonly featureSnapshotFn: lambda.Function;

  constructor(scope: Construct, id: string, props: MLStackProps) {
    super(scope, id, props);

    // Flip-back switch: false (default) uses the container Lambda trainer;
    // true uses the managed SageMaker training job (needs training quota > 0).
    // Deploy with: cdk deploy -c useSagemakerTraining=true
    const useSagemakerTraining =
      this.node.tryGetContext('useSagemakerTraining') === true ||
      this.node.tryGetContext('useSagemakerTraining') === 'true';

    // ---- feature-snapshot Lambda (5-min schedule) ----
    const functionName = 'la-metro-feature-snapshot';
    const logGroup = new logs.LogGroup(this, 'FeatureSnapshotFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.featureSnapshotFn = new lambda.Function(this, 'FeatureSnapshotFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'feature_snapshot', '.build'),
      ),
      memorySize: 256,
      // 30s: GSI query + Open-Meteo (≤4s) + gzip + one S3 PUT is well under
      // 5s in practice. Generous cushion for cold start + slow weather call.
      timeout: cdk.Duration.seconds(30),
      environment: {
        ROUTE_AGGREGATES_TABLE_NAME: props.routeAggregatesTable.tableName,
        ROUTE_AGGREGATES_WINDOW_GSI: 'window_start_iso-index',
        WEATHER_CACHE_TABLE_NAME: props.weatherCacheTable.tableName,
        ARCHIVE_BUCKET: props.archiveBucket.bucketName,
        PROCESSED_FEATURES_PREFIX: 'processed-features',
        WEATHER_CACHE_TTL_SECONDS: '600',
      },
      logGroup,
      description: 'Phase 7a: durable per-(route, window) feature snapshots + weather.',
    });

    // GSI read on route-aggregates — CDK's grantReadData only covers the base
    // table, so explicitly grant Query on the index resource ARN.
    props.routeAggregatesTable.grantReadData(this.featureSnapshotFn);
    this.featureSnapshotFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['dynamodb:Query'],
      resources: [`${props.routeAggregatesTable.tableArn}/index/window_start_iso-index`],
    }));

    props.weatherCacheTable.grantWriteData(this.featureSnapshotFn);

    // S3 write scoped to the processed-features/ prefix only — the Lambda has
    // no business touching raw-events/, gtfs-static/, or models/.
    props.archiveBucket.grantPut(this.featureSnapshotFn, 'processed-features/*');

    new events.Rule(this, 'FeatureSnapshotSchedule', {
      ruleName: 'la-metro-feature-snapshot-schedule',
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [new targets.LambdaFunction(this.featureSnapshotFn)],
      description: 'Triggers feature-snapshot every 5 min.',
    });

    // ---- Glue catalog over processed-features/ ----
    // Partition projection avoids needing a crawler: Athena infers the
    // partition values from a date range we declare here. The catalog only
    // stores the table definition; we never pay crawler runtime cost.
    const glueDb = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'la_metro',
        description: 'LA Metro reliability platform — Athena/Glue catalog.',
      },
    });

    const glueTable = new glue.CfnTable(this, 'RouteWindowFeaturesTable', {
      catalogId: this.account,
      databaseName: 'la_metro',
      tableInput: {
        name: 'route_window_features',
        description:
          'Per-(route, window) snapshots written by the feature-snapshot Lambda.',
        tableType: 'EXTERNAL_TABLE',
        parameters: {
          classification: 'json',
          // Partition projection — Athena auto-generates partition values.
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2026,2035',
          'projection.month.type': 'integer',
          'projection.month.range': '1,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '1,31',
          'projection.day.digits': '2',
          'projection.hour.type': 'integer',
          'projection.hour.range': '0,23',
          'projection.hour.digits': '2',
          'storage.location.template':
            `s3://${props.archiveBucket.bucketName}/processed-features/` +
            'year=${year}/month=${month}/day=${day}/hour=${hour}/',
        },
        partitionKeys: [
          { name: 'year', type: 'int' },
          { name: 'month', type: 'int' },
          { name: 'day', type: 'int' },
          { name: 'hour', type: 'int' },
        ],
        storageDescriptor: {
          location: `s3://${props.archiveBucket.bucketName}/processed-features/`,
          inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
          outputFormat:
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.openx.data.jsonserde.JsonSerDe',
            parameters: { 'ignore.malformed.json': 'true' },
          },
          columns: [
            { name: 'route_id', type: 'string' },
            { name: 'window_start_iso', type: 'string' },
            { name: 'avg_delay_seconds', type: 'int' },
            { name: 'p95_delay_seconds', type: 'int' },
            { name: 'on_time_pct', type: 'double' },
            { name: 'vehicle_count', type: 'int' },
            { name: 'temp_c', type: 'double' },
            { name: 'precip_mm', type: 'double' },
            { name: 'weather_observed_at', type: 'string' },
            { name: 'ingested_at', type: 'string' },
          ],
          compressed: true,
        },
      },
    });
    glueTable.addDependency(glueDb);

    new cdk.CfnOutput(this, 'GlueDatabaseName', { value: 'la_metro' });
    new cdk.CfnOutput(this, 'GlueTableName', { value: 'route_window_features' });

    new cdk.CfnOutput(this, 'FeatureSnapshotFnName', {
      value: this.featureSnapshotFn.functionName,
    });

    // ================================================================
    // Phase 7b — nightly training pipeline (Step Functions).
    // ================================================================

    // ---- data_sufficiency_check Lambda ----
    // Reads the UNLOAD query's exact output-row count from Athena
    // (GetQueryRuntimeStatistics) and gates training on it.
    const sufficiencyName = 'la-metro-data-sufficiency-check';
    const sufficiencyLog = new logs.LogGroup(this, 'SufficiencyFnLogs', {
      logGroupName: `/aws/lambda/${sufficiencyName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const sufficiencyFn = new lambda.Function(this, 'SufficiencyFn', {
      functionName: sufficiencyName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'data_sufficiency_check', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(15),
      environment: { DEFAULT_THRESHOLD_ROWS: '1000' },
      logGroup: sufficiencyLog,
      description: 'Phase 7b: reads UNLOAD output-row count from Athena for the gate.',
    });
    sufficiencyFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['athena:GetQueryRuntimeStatistics'],
      resources: ['*'],   // query-execution ARNs aren't known until run time
    }));

    // ---- evaluate_model Lambda ----
    const evalName = 'la-metro-evaluate-model';
    const evalLog = new logs.LogGroup(this, 'EvaluateFnLogs', {
      logGroupName: `/aws/lambda/${evalName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const evaluateFn = new lambda.Function(this, 'EvaluateFn', {
      functionName: evalName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'evaluate_model', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(20),
      environment: { VALIDATION_METRIC_NAME: 'validation:rmse' },
      logGroup: evalLog,
      description: 'Phase 7b: compares candidate training-job metric vs deployed model.',
    });
    props.archiveBucket.grantRead(evaluateFn, 'models/*');
    evaluateFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sagemaker:DescribeTrainingJob'],
      resources: ['*'],   // training job ARNs include the run-id we won't know upfront
    }));

    // ---- promote_model Lambda ----
    const promoteName = 'la-metro-promote-model';
    const promoteLog = new logs.LogGroup(this, 'PromoteFnLogs', {
      logGroupName: `/aws/lambda/${promoteName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const promoteFn = new lambda.Function(this, 'PromoteFn', {
      functionName: promoteName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'promote_model', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      environment: {},
      logGroup: promoteLog,
      description: 'Phase 7b: copy candidate artifact to versioned + current/, write metrics.json.',
    });
    // Promote reads candidates from training-jobs/ and writes to models/.
    props.archiveBucket.grantRead(promoteFn, 'training-jobs/*');
    props.archiveBucket.grantReadWrite(promoteFn, 'models/*');

    // ---- SageMaker training-job execution role ----
    // Reads training-sets/ (CSV input) and writes training-jobs/<job>/output/
    // in the same archive bucket.
    const trainingRole = new iam.Role(this, 'SageMakerTrainingRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      description: 'Phase 7b: SageMaker training job execution role.',
    });
    props.archiveBucket.grantRead(trainingRole, 'training-sets/*');
    props.archiveBucket.grantReadWrite(trainingRole, 'training-jobs/*');
    trainingRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
      ],
      resources: [`arn:aws:logs:${this.region}:${this.account}:log-group:/aws/sagemaker/TrainingJobs:*`],
    }));

    // ---- train_model container Lambda (XGBoost trainer) ----
    // Container image because xgboost+numpy exceed the zip layer limit.
    // x86_64 so CI builds natively and the artifact matches the x86 SageMaker
    // XGBoost inference container it serves with. Only created in Lambda
    // mode — when flipped to SageMaker training we skip building the image.
    let trainFn: lambda.DockerImageFunction | undefined;
    if (!useSagemakerTraining) {
      const trainName = 'la-metro-train-model';
      const trainLog = new logs.LogGroup(this, 'TrainModelFnLogs', {
        logGroupName: `/aws/lambda/${trainName}`,
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      trainFn = new lambda.DockerImageFunction(this, 'TrainModelFn', {
        functionName: trainName,
        code: lambda.DockerImageCode.fromImageAsset(
          path.join(__dirname, '..', '..', 'lambdas', 'train_model'),
          { platform: Platform.LINUX_AMD64 },
        ),
        architecture: lambda.Architecture.X86_64,
        memorySize: 3008,            // headroom for xgboost; trains in seconds
        timeout: cdk.Duration.minutes(5),
        logGroup: trainLog,
        description: 'Lambda XGBoost trainer (SageMaker training quota fallback).',
      });
      props.archiveBucket.grantRead(trainFn, 'training-sets/*');
      props.archiveBucket.grantReadWrite(trainFn, 'training-jobs/*');
    }

    // ---- Step Functions state machine ----
    const sfnLog = new logs.LogGroup(this, 'NightlyTrainingSfnLogs', {
      logGroupName: '/aws/vendedlogs/states/la-metro-nightly-training',
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const athenaWorkgroup = 'primary';
    const athenaResultsPrefix = `s3://${props.archiveBucket.bucketName}/athena-results/`;
    const archiveBucketUri = `s3://${props.archiveBucket.bucketName}`;

    // Build the Athena UNLOAD query string as a States.Format() intrinsic.
    // We strip line comments and collapse whitespace so the SQL is a single
    // line safe to embed (our SQL has no '--' or braces inside string
    // literals, so this is lossless). ${ARCHIVE_BUCKET} is bound here at
    // deploy time; ${RUN_ID} becomes the {} placeholder Step Functions fills
    // with the execution name at run time.
    const sqlRaw = fs.readFileSync(
      path.join(__dirname, '..', '..', 'ml', 'feature_extraction.sql'),
      'utf-8',
    );
    const sqlOneLine = sqlRaw
      .split('\n')
      .map((line: string) => line.replace(/--.*$/, ''))
      .join(' ')
      .replace(/\s+/g, ' ')
      .trim()
      .replace('${ARCHIVE_BUCKET}', props.archiveBucket.bucketName);
    const sqlForFormat = sqlOneLine
      .replace(/\\/g, '\\\\')
      .replace(/'/g, "\\'")
      .replace('${RUN_ID}', '{}');
    const extractQueryString = `States.Format('${sqlForFormat}', $.context.run_id)`;

    const sagemakerTrainState = {
      Type: 'Task',
      Resource: 'arn:aws:states:::sagemaker:createTrainingJob.sync',
      Parameters: {
        'TrainingJobName.$':
          "States.Format('la-metro-delay-{}', $.context.run_id)",
        AlgorithmSpecification: {
          // us-west-2 account ID (246618743249); see xgboostImage const below.
          TrainingImage: '246618743249.dkr.ecr.us-west-2.amazonaws.com/sagemaker-xgboost:1.7-1',
          TrainingInputMode: 'File',
          MetricDefinitions: [
            { Name: 'validation:rmse', Regex: '.*\\[.*\\]#011validation-rmse:([0-9\\.]+).*' },
            { Name: 'train:rmse',      Regex: '.*\\[.*\\]#011train-rmse:([0-9\\.]+).*' },
          ],
        },
        RoleArn: trainingRole.roleArn,
        ResourceConfig: { InstanceType: 'ml.m5.large', InstanceCount: 1, VolumeSizeInGB: 10 },
        StoppingCondition: { MaxRuntimeInSeconds: 600 },
        HyperParameters: {
          objective: 'reg:squarederror', num_round: '200',
          max_depth: '6', eta: '0.1', subsample: '0.8',
        },
        InputDataConfig: [{
          ChannelName: 'train',
          DataSource: {
            S3DataSource: {
              S3DataType: 'S3Prefix',
              'S3Uri.$':
                "States.Format('{}/training-sets/run={}/', '" +
                archiveBucketUri + "', $.context.run_id)",
              S3DataDistributionType: 'FullyReplicated',
            },
          },
          ContentType: 'text/csv',
          CompressionType: 'Gzip',
        }],
        OutputDataConfig: {
          'S3OutputPath.$':
            "States.Format('{}/training-jobs/run={}/', '" +
            archiveBucketUri + "', $.context.run_id)",
        },
      },
      ResultPath: '$.training',
      Next: 'Evaluate',
      Catch: [{ ErrorEquals: ['States.ALL'], Next: 'FailedTerminal' }],
    };

    const lambdaTrainState = {
      Type: 'Task',
      Resource: 'arn:aws:states:::lambda:invoke',
      Parameters: {
        FunctionName: trainFn ? trainFn.functionArn : '',
        Payload: {
          'training_set_uri.$':
            "States.Format('{}/training-sets/run={}/', '" +
            archiveBucketUri + "', $.context.run_id)",
          'output_model_uri.$':
            "States.Format('{}/training-jobs/run={}/output/model.tar.gz', '" +
            archiveBucketUri + "', $.context.run_id)",
        },
      },
      ResultSelector: { 'result.$': '$.Payload' },
      ResultPath: '$.training',
      Next: 'Evaluate',
      Catch: [{ ErrorEquals: ['States.ALL'], Next: 'FailedTerminal' }],
    };

    const trainState = useSagemakerTraining ? sagemakerTrainState : lambdaTrainState;

    const evaluatePayload = useSagemakerTraining
      ? {
          'training_job_name.$': '$.training.TrainingJobName',
          'models_prefix_uri': `${archiveBucketUri}/models/`,
        }
      : {
          'candidate_metric.$': '$.training.result.candidate_metric',
          'candidate_model_uri.$': '$.training.result.candidate_model_uri',
          'metric_name.$': '$.training.result.metric_name',
          'models_prefix_uri': `${archiveBucketUri}/models/`,
        };

    // ---- precompute-predictions Lambda + 5-min schedule ----
    const preName = 'la-metro-precompute-predictions';
    const preLog = new logs.LogGroup(this, 'PrecomputeFnLogs', {
      logGroupName: `/aws/lambda/${preName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const endpointName = 'la-metro-delay-predictor';
    const precomputeFn = new lambda.Function(this, 'PrecomputeFn', {
      functionName: preName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'precompute_predictions', '.build'),
      ),
      memorySize: 512,
      // 60s: scan + 150 endpoint invokes + 150 DDB puts. Plenty of cushion.
      timeout: cdk.Duration.seconds(60),
      environment: {
        ROUTE_AGGREGATES_TABLE_NAME: props.routeAggregatesTable.tableName,
        ROUTE_PREDICTIONS_TABLE_NAME: props.routePredictionsTable.tableName,
        WEATHER_CACHE_TABLE_NAME: props.weatherCacheTable.tableName,
        MODELS_PREFIX_URI: `s3://${props.archiveBucket.bucketName}/models`,
        SAGEMAKER_ENDPOINT_NAME: endpointName,
        PREDICTION_TTL_SECONDS: '900',
      },
      logGroup: preLog,
      description: 'Phase 7c: per-route prediction precompute (5 min).',
    });
    // grantReadData covers Query + Scan + GetItem on the BASE table, which is
    // all the precompute Lambda needs (route-aggregates PK is route_id; no GSI).
    props.routeAggregatesTable.grantReadData(precomputeFn);
    props.routePredictionsTable.grantWriteData(precomputeFn);
    props.weatherCacheTable.grantReadData(precomputeFn);
    props.archiveBucket.grantRead(precomputeFn, 'models/*');
    precomputeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sagemaker:InvokeEndpoint'],
      resources: [
        `arn:aws:sagemaker:${this.region}:${this.account}:endpoint/${endpointName}`,
      ],
    }));

    new events.Rule(this, 'PrecomputeSchedule', {
      ruleName: 'la-metro-precompute-predictions-schedule',
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [new targets.LambdaFunction(precomputeFn)],
      description: 'Phase 7c: triggers precompute every 5 min.',
    });

    // ---- SageMaker Serverless endpoint ----
    const sagemakerExecRole = new iam.Role(this, 'SageMakerExecutionRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      description: 'Phase 7c: SageMaker endpoint execution role (reads models/*).',
    });
    props.archiveBucket.grantRead(sagemakerExecRole, 'models/*');
    sagemakerExecRole.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: ['*'],
    }));

    const xgboostImage =
      // us-west-2 SageMaker XGBoost framework image. NOTE: the account ID is
      // region-specific — 246618743249 is us-west-2 (746614075791 is us-west-1).
      '246618743249.dkr.ecr.us-west-2.amazonaws.com/sagemaker-xgboost:1.7-1';

    // The Model points at models/current/model.tar.gz which must already
    // exist (produced by 7b's first successful pipeline run). If you deploy
    // 7c before 7b has run, CreateModel will fail with NoSuchKey.
    const initialModel = new cdk.aws_sagemaker.CfnModel(this, 'InitialModel', {
      modelName: 'la-metro-delay-predictor-initial',
      executionRoleArn: sagemakerExecRole.roleArn,
      primaryContainer: {
        image: xgboostImage,
        modelDataUrl: `s3://${props.archiveBucket.bucketName}/models/current/model.tar.gz`,
      },
    });

    const initialEndpointConfig = new cdk.aws_sagemaker.CfnEndpointConfig(
      this, 'InitialEndpointConfig', {
        endpointConfigName: 'la-metro-delay-predictor-cfg-initial',
        productionVariants: [{
          variantName: 'AllTraffic',
          modelName: initialModel.attrModelName,
          serverlessConfig: { memorySizeInMb: 1024, maxConcurrency: 5 },
        }],
      },
    );
    initialEndpointConfig.addDependency(initialModel);

    const endpoint = new cdk.aws_sagemaker.CfnEndpoint(this, 'Endpoint', {
      endpointName,
      endpointConfigName: initialEndpointConfig.attrEndpointConfigName,
    });
    endpoint.addDependency(initialEndpointConfig);

    new cdk.CfnOutput(this, 'SagemakerEndpointName', { value: endpointName });

    // ---- update_endpoint Lambda (called from Step Functions after Promote) ----
    const upName = 'la-metro-update-endpoint';
    const upLog = new logs.LogGroup(this, 'UpdateEndpointFnLogs', {
      logGroupName: `/aws/lambda/${upName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const updateEndpointFn = new lambda.Function(this, 'UpdateEndpointFn', {
      functionName: upName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'update_endpoint', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      environment: {
        SAGEMAKER_ENDPOINT_NAME: endpointName,
        TRAINING_IMAGE: xgboostImage,
        SAGEMAKER_EXECUTION_ROLE_ARN: sagemakerExecRole.roleArn,
        ENDPOINT_MEMORY_MB: '1024',
        ENDPOINT_MAX_CONCURRENCY: '5',
      },
      logGroup: upLog,
      description: 'Phase 7c: refresh SageMaker endpoint to a freshly-promoted model.',
    });
    updateEndpointFn.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'sagemaker:CreateModel', 'sagemaker:CreateEndpointConfig',
        'sagemaker:UpdateEndpoint',
      ],
      resources: ['*'],
    }));
    updateEndpointFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['iam:PassRole'],
      resources: [sagemakerExecRole.roleArn],
    }));

    const definition = {
      Comment: 'Phase 7b nightly training pipeline.',
      StartAt: 'GenerateRunId',
      States: {
        GenerateRunId: {
          Type: 'Pass',
          Parameters: { 'run_id.$': '$$.Execution.Name' },
          ResultPath: '$.context',
          Next: 'ExtractFeatures',
        },
        ExtractFeatures: {
          Type: 'Task',
          Resource: 'arn:aws:states:::athena:startQueryExecution.sync',
          Parameters: {
            'QueryString.$': extractQueryString,
            WorkGroup: athenaWorkgroup,
            ResultConfiguration: { OutputLocation: athenaResultsPrefix },
          },
          ResultPath: '$.athena',
          Next: 'CheckSufficiency',
          Catch: [{ ErrorEquals: ['States.ALL'], Next: 'FailedTerminal' }],
        },
        CheckSufficiency: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: sufficiencyFn.functionArn,
            Payload: {
              'query_execution_id.$': '$.athena.QueryExecution.QueryExecutionId',
            },
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.sufficiency',
          Next: 'BranchOnSufficiency',
        },
        BranchOnSufficiency: {
          Type: 'Choice',
          Choices: [{
            Variable: '$.sufficiency.result.sufficient',
            BooleanEquals: true,
            Next: 'Train',
          }],
          Default: 'SkipTraining',
        },
        SkipTraining: {
          Type: 'Succeed',
          Comment: 'Insufficient data; gracefully skipped this run.',
        },
        Train: trainState,
        Evaluate: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: evaluateFn.functionArn,
            Payload: evaluatePayload,
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.eval',
          Next: 'BranchOnEval',
        },
        BranchOnEval: {
          Type: 'Choice',
          Choices: [{
            Variable: '$.eval.result.promote',
            BooleanEquals: true,
            Next: 'Promote',
          }],
          Default: 'SkippedPromotion',
        },
        SkippedPromotion: {
          Type: 'Succeed',
          Comment: 'Candidate did not beat deployed model; not promoted.',
        },
        Promote: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: promoteFn.functionArn,
            Payload: {
              'candidate_model_uri.$': '$.eval.result.candidate_model_uri',
              'models_prefix_uri': `${archiveBucketUri}/models`,
              'candidate_metric.$': '$.eval.result.candidate_metric',
              'metric_name.$': '$.eval.result.metric_name',
            },
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.promote',
          Next: 'UpdateEndpoint',
        },
        UpdateEndpoint: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: updateEndpointFn.functionArn,
            Payload: {
              'promoted_version.$': '$.promote.result.promoted_version',
              'current_model_uri.$': '$.promote.result.current_model_uri',
            },
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.endpoint',
          End: true,
        },
        FailedTerminal: {
          Type: 'Fail',
          Cause: 'A state in the nightly training pipeline failed; see CloudWatch.',
        },
      },
    };

    const sfnRole = new iam.Role(this, 'NightlyTrainingSfnRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      inlinePolicies: {
        Inline: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'athena:StartQueryExecution',
                'athena:GetQueryExecution',
                'athena:GetQueryResults',
                'athena:StopQueryExecution',
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              actions: ['glue:GetTable', 'glue:GetDatabase', 'glue:GetPartitions'],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              actions: ['s3:GetObject', 's3:PutObject', 's3:ListBucket', 's3:GetBucketLocation'],
              resources: [props.archiveBucket.bucketArn, `${props.archiveBucket.bucketArn}/*`],
            }),
            new iam.PolicyStatement({
              // AddTags + ListTags are required by the createTrainingJob.sync
              // integration, which tags the job for completion tracking.
              actions: [
                'sagemaker:CreateTrainingJob', 'sagemaker:DescribeTrainingJob',
                'sagemaker:StopTrainingJob', 'sagemaker:AddTags', 'sagemaker:ListTags',
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              actions: ['iam:PassRole'],
              resources: [trainingRole.roleArn],
            }),
            new iam.PolicyStatement({
              actions: ['lambda:InvokeFunction'],
              resources: [
                sufficiencyFn.functionArn,
                evaluateFn.functionArn,
                promoteFn.functionArn,
                updateEndpointFn.functionArn,
                ...(trainFn ? [trainFn.functionArn] : []),
              ],
            }),
            new iam.PolicyStatement({
              actions: ['events:PutTargets', 'events:PutRule', 'events:DescribeRule'],
              resources: ['*'],   // required by the .sync callback pattern
            }),
            new iam.PolicyStatement({
              actions: ['logs:CreateLogDelivery', 'logs:GetLogDelivery', 'logs:UpdateLogDelivery',
                        'logs:DeleteLogDelivery', 'logs:ListLogDeliveries',
                        'logs:PutResourcePolicy', 'logs:DescribeResourcePolicies', 'logs:DescribeLogGroups'],
              resources: ['*'],
            }),
          ],
        }),
      },
    });

    const stateMachine = new sfn.CfnStateMachine(this, 'NightlyTrainingSfn', {
      stateMachineName: 'la-metro-nightly-training',
      roleArn: sfnRole.roleArn,
      definitionString: JSON.stringify(definition),
      loggingConfiguration: {
        destinations: [{ cloudWatchLogsLogGroup: { logGroupArn: sfnLog.logGroupArn } }],
        level: 'ALL',
        includeExecutionData: true,
      },
    });

    // ---- EventBridge Scheduler — daily at 10:00 UTC (~03:00 PT) ----
    new scheduler.CfnSchedule(this, 'NightlyTrainingSchedule', {
      name: 'la-metro-nightly-training',
      scheduleExpression: 'cron(0 10 * * ? *)',
      flexibleTimeWindow: { mode: 'OFF' },
      target: {
        arn: stateMachine.attrArn,
        roleArn: new iam.Role(this, 'NightlyScheduleRole', {
          assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
          inlinePolicies: {
            Inline: new iam.PolicyDocument({
              statements: [new iam.PolicyStatement({
                actions: ['states:StartExecution'],
                resources: [stateMachine.attrArn],
              })],
            }),
          },
        }).roleArn,
        input: '{}',
      },
      description: 'Phase 7b: triggers the nightly training pipeline.',
    });

    new cdk.CfnOutput(this, 'NightlyTrainingStateMachineArn', {
      value: stateMachine.attrArn,
    });
  }
}
