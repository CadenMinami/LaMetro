import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

export interface IngestionStackProps extends cdk.StackProps {
  laMetroApiKey?: string;
  laMetroFeedUrl?: string;
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

    const ingestionFn = new lambda.Function(this, 'IngestionFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(lambdaAssetPath),
      memorySize: 512,
      timeout: cdk.Duration.seconds(15),
      environment: {
        LA_METRO_API_KEY: props.laMetroApiKey ?? '',
        ...(props.laMetroFeedUrl ? { LA_METRO_FEED_URL: props.laMetroFeedUrl } : {}),
      },
      logGroup,
      description: 'Phase 1: fetches LA Metro GTFS-RT, logs vehicle count.',
    });

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
