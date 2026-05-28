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

export interface MLStackProps extends cdk.StackProps {
  routeAggregatesTable: dynamodb.ITable;
  weatherCacheTable: dynamodb.ITable;
  archiveBucket: s3.IBucket;
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

    new cdk.CfnOutput(this, 'FeatureSnapshotFnName', {
      value: this.featureSnapshotFn.functionName,
    });
  }
}
