import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

export interface ApiStackProps extends cdk.StackProps {
  hotVehiclesTable: dynamodb.ITable;
}

/**
 * Phase 2 read API:
 *   GET /vehicles?bbox=lon_min,lat_min,lon_max,lat_max[&route_id=X][&limit=N]
 *
 * Backed by a Query Lambda + REST API Gateway. CORS open to '*' for now —
 * tighten to specific origins in Phase 9.
 */
export class ApiStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    const lambdaAssetPath = path.join(__dirname, '..', '..', 'lambdas', 'query_api', '.build');
    const functionName = 'la-metro-query-api';

    const logGroup = new logs.LogGroup(this, 'QueryFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const queryFn = new lambda.Function(this, 'QueryFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(lambdaAssetPath),
      memorySize: 512,
      timeout: cdk.Duration.seconds(10),
      environment: {
        HOT_VEHICLES_TABLE_NAME: props.hotVehiclesTable.tableName,
        GEOHASH_PRECISION: '6',
      },
      logGroup,
      description: 'Phase 2: REST query for live vehicles in a bbox.',
    });

    props.hotVehiclesTable.grantReadData(queryFn);

    const api = new apigw.LambdaRestApi(this, 'QueryApi', {
      restApiName: 'la-metro-query-api',
      handler: queryFn,
      // Auto-wires every path/method to the lambda. We add /vehicles below.
      proxy: false,
      deployOptions: {
        stageName: 'prod',
        throttlingBurstLimit: 100,
        throttlingRateLimit: 60,  // 60 req/s account default; per-IP throttling added in Phase 9
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigw.Cors.ALL_ORIGINS,
        allowMethods: ['GET', 'OPTIONS'],
        allowHeaders: ['Content-Type'],
      },
    });

    const vehicles = api.root.addResource('vehicles');
    vehicles.addMethod('GET');

    new cdk.CfnOutput(this, 'ApiUrl', {
      value: api.url,
      description: 'Base URL for the LA Metro query API.',
    });
  }
}
