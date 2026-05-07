import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as eventsources from 'aws-cdk-lib/aws-lambda-event-sources';

export interface ProcessingStackProps extends cdk.StackProps {
  vehicleStream: kinesis.IStream;
  hotVehiclesTable: dynamodb.ITable;
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
  }
}
