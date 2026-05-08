import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as apigwv2 from 'aws-cdk-lib/aws-apigatewayv2';
import * as integrations from 'aws-cdk-lib/aws-apigatewayv2-integrations';

export interface WebSocketStackProps extends cdk.StackProps {
  connectionsTable: dynamodb.ITable;
}

/**
 * Phase 5a: WebSocket API + connection manager Lambda.
 *
 * Three routes hit the same Lambda; it dispatches on event.requestContext.routeKey.
 * Keeping it one function is a deliberate simplicity choice — the routes
 * share env vars, deps, and IAM, and the dispatch is two lines of Python.
 */
export class WebSocketStack extends cdk.Stack {
  public readonly api: apigwv2.WebSocketApi;
  public readonly stage: apigwv2.WebSocketStage;
  /** wss://… URL the browser connects to. */
  public readonly clientUrl: string;
  /** https://….execute-api…/prod URL the broadcast Lambda posts to. */
  public readonly callbackUrl: string;

  constructor(scope: Construct, id: string, props: WebSocketStackProps) {
    super(scope, id, props);

    const lambdaAssetPath = path.join(__dirname, '..', '..', 'lambdas', 'websocket', '.build');
    const functionName = 'la-metro-websocket-connections';

    const logGroup = new logs.LogGroup(this, 'WsConnFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const wsConnFn = new lambda.Function(this, 'WsConnFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(lambdaAssetPath),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      environment: {
        CONNECTIONS_TABLE_NAME: props.connectionsTable.tableName,
        // 2h TTL — far longer than typical browser-tab lifetime, short
        // enough to garbage-collect orphaned rows from missed disconnects.
        CONNECTION_TTL_SECONDS: '7200',
      },
      logGroup,
      description: 'Phase 5a: WebSocket $connect / $disconnect / subscribe.',
    });

    props.connectionsTable.grantReadWriteData(wsConnFn);

    // CDK's WebSocketLambdaIntegration creates the Lambda invoke permission
    // exactly once per integration *instance*, so reusing one across routes
    // means only the first route gets permission. Build a fresh integration
    // per route instead.
    const mkIntegration = (id: string) =>
      new integrations.WebSocketLambdaIntegration(id, wsConnFn);

    this.api = new apigwv2.WebSocketApi(this, 'WsApi', {
      apiName: 'la-metro-websockets',
      connectRouteOptions: { integration: mkIntegration('WsConnIntegrationConnect') },
      disconnectRouteOptions: { integration: mkIntegration('WsConnIntegrationDisconnect') },
      defaultRouteOptions: { integration: mkIntegration('WsConnIntegrationDefault') },
    });

    // Custom action: client sends {"action": "subscribe", ...}. Route key
    // == the action value.
    this.api.addRoute('subscribe', {
      integration: mkIntegration('WsConnIntegrationSubscribe'),
    });

    this.stage = new apigwv2.WebSocketStage(this, 'WsStage', {
      webSocketApi: this.api,
      stageName: 'prod',
      autoDeploy: true,
    });

    // The browser-facing URL is wss://<api-id>.execute-api.<region>.amazonaws.com/<stage>
    // The server-side management URL (used by the broadcast Lambda) is the
    // same host with https://. We expose both so other stacks don't have to
    // reconstruct them by string-stitching.
    this.clientUrl = this.stage.url; // wss://...
    this.callbackUrl = this.stage.callbackUrl; // https://...

    new cdk.CfnOutput(this, 'WsClientUrl', { value: this.clientUrl });
    new cdk.CfnOutput(this, 'WsCallbackUrl', { value: this.callbackUrl });
    new cdk.CfnOutput(this, 'WsConnFnName', { value: wsConnFn.functionName });
  }
}
