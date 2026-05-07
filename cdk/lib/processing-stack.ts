import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as eventsources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

export interface ProcessingStackProps extends cdk.StackProps {
  vehicleStream: kinesis.IStream;
  hotVehiclesTable: dynamodb.ITable;
  routeAggregatesTable: dynamodb.ITable;
}

/**
 * Phase 2 processing tier:
 *   - Enrichment Lambda — Kinesis-triggered, writes raw positions to
 *     hot-vehicles. (Real schedule-deviation enrichment lands in Phase 4.)
 *
 * Future Lambdas (aggregation, alerts, inference) will join this stack as
 * later phases come online.
 */
export class ProcessingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ProcessingStackProps) {
    super(scope, id, props);

    const lambdaAssetPath = path.join(__dirname, '..', '..', 'lambdas', 'enrichment', '.build');
    const functionName = 'la-metro-enrichment';

    const logGroup = new logs.LogGroup(this, 'EnrichmentFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const enrichmentFn = new lambda.Function(this, 'EnrichmentFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(lambdaAssetPath),
      memorySize: 512,
      timeout: cdk.Duration.seconds(60),
      environment: {
        HOT_VEHICLES_TABLE_NAME: props.hotVehiclesTable.tableName,
        GEOHASH_PRECISION: '6',
        HOT_VEHICLE_TTL_SECONDS: '3600',
      },
      logGroup,
      description: 'Phase 2: Kinesis trigger → hot-vehicles DynamoDB. No delay calc yet.',
    });

    props.hotVehiclesTable.grantWriteData(enrichmentFn);

    enrichmentFn.addEventSource(
      new eventsources.KinesisEventSource(props.vehicleStream, {
        // TRIM_HORIZON: on first deploy, start at the oldest record. Subsequent
        // restarts pick up where we left off (via the consumer checkpoint).
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        batchSize: 100,
        maxBatchingWindow: cdk.Duration.seconds(5),
        // If a poison record blows up the batch, retry up to 3 times then move
        // on. Without this a single bad record halts the shard forever.
        retryAttempts: 3,
        bisectBatchOnError: true,
        // Report partial batch failures so successful records aren't redelivered.
        reportBatchItemFailures: true,
      }),
    );

    new cdk.CfnOutput(this, 'EnrichmentFnName', { value: enrichmentFn.functionName });

    // ----- Phase 4b: Aggregation Lambda -----
    // Triggered every minute. Scans hot-vehicles, groups by route, writes
    // 5-min-bucket rolling stats to route-aggregates.
    const aggAssetPath = path.join(__dirname, '..', '..', 'lambdas', 'aggregation', '.build');
    const aggFunctionName = 'la-metro-aggregation';

    const aggLogGroup = new logs.LogGroup(this, 'AggregationFnLogs', {
      logGroupName: `/aws/lambda/${aggFunctionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const aggregationFn = new lambda.Function(this, 'AggregationFn', {
      functionName: aggFunctionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(aggAssetPath),
      memorySize: 512,
      // 60s budget — DDB scan + per-route writes for ~150 routes is well under
      // 10s in practice. Cushion is for cold starts.
      timeout: cdk.Duration.seconds(60),
      environment: {
        HOT_VEHICLES_TABLE_NAME: props.hotVehiclesTable.tableName,
        ROUTE_AGGREGATES_TABLE_NAME: props.routeAggregatesTable.tableName,
      },
      logGroup: aggLogGroup,
      description: 'Phase 4b: rolling per-route stats every minute.',
    });

    props.hotVehiclesTable.grantReadData(aggregationFn);
    props.routeAggregatesTable.grantWriteData(aggregationFn);

    // EventBridge: rate(1 minute). Cron would also work but rate is simpler
    // for "every N minutes" without needing AWS's UTC cron expressions.
    new events.Rule(this, 'AggregationSchedule', {
      ruleName: 'la-metro-aggregation-schedule',
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
      targets: [new targets.LambdaFunction(aggregationFn)],
      description: 'Triggers the Aggregation Lambda once per minute.',
    });

    new cdk.CfnOutput(this, 'AggregationFnName', { value: aggregationFn.functionName });
  }
}
