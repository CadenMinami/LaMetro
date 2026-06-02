import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as iam from 'aws-cdk-lib/aws-iam';

export interface IngestionStackProps extends cdk.StackProps {
  swiftlySecretName: string;
  laMetroFeedUrl?: string;
  vehicleStream: kinesis.IStream;
  // Scale-to-zero gate: ingestion scans this table for active WebSocket
  // connections and skips the cycle when nobody is watching. Passed by name
  // (not as a Table ref) so this stack stays decoupled from StorageStack and
  // deployable on its own — the ARN is constructed locally below.
  connectionsTableName: string;
}

export class IngestionStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: IngestionStackProps) {
    super(scope, id, props);

    const lambdaAssetPath = path.join(__dirname, '..', '..', 'lambdas', 'ingestion', '.build');
    const functionName = 'la-metro-ingestion';

    const logGroup = new logs.LogGroup(this, 'IngestionFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const swiftlySecret = secretsmanager.Secret.fromSecretNameV2(
      this,
      'SwiftlySecret',
      props.swiftlySecretName,
    );

    const ingestionFn = new lambda.Function(this, 'IngestionFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(lambdaAssetPath),
      memorySize: 512,
      timeout: cdk.Duration.seconds(30),
      environment: {
        SWIFTLY_SECRET_NAME: props.swiftlySecretName,
        VEHICLE_STREAM_NAME: props.vehicleStream.streamName,
        CONNECTIONS_TABLE_NAME: props.connectionsTableName,
        ...(props.laMetroFeedUrl ? { LA_METRO_FEED_URL: props.laMetroFeedUrl } : {}),
      },
      logGroup,
      description: 'Phase 2: fetches LA Metro GTFS-RT, emits one Kinesis record per vehicle.',
    });

    swiftlySecret.grantRead(ingestionFn);
    props.vehicleStream.grantWrite(ingestionFn);
    // Least privilege: ingestion only needs to count connections, not read them.
    // ARN built from this stack's region/account + the known table name, so we
    // grant access without importing the table from StorageStack.
    ingestionFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['dynamodb:Scan'],
        resources: [
          this.formatArn({
            service: 'dynamodb',
            resource: 'table',
            resourceName: props.connectionsTableName,
          }),
        ],
      }),
    );

    const rule = new events.Rule(this, 'IngestionSchedule', {
      ruleName: 'la-metro-ingestion-every-minute',
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
      description: 'Triggers the ingestion Lambda every 60 seconds.',
    });
    rule.addTarget(new targets.LambdaFunction(ingestionFn));

    new cdk.CfnOutput(this, 'IngestionFnName', {
      value: ingestionFn.functionName,
      description: 'Name of the ingestion Lambda — tail logs with `aws logs tail /aws/lambda/$NAME --follow`.',
    });
  }
}
