import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

export interface AuthStackProps extends cdk.StackProps {
  usersTable: dynamodb.ITable;
}

/**
 * Phase 6 auth tier:
 *   - Cognito user pool (email sign-up, self-service, email verification)
 *   - App client for the SPA (no client secret — public client, SRP flow)
 *   - PostConfirmation Lambda trigger that seeds the users table
 *
 * The frontend talks to this pool directly via the Amplify Authenticator
 * (SRP), so we don't need a Hosted UI domain or OAuth flows.
 */
export class AuthStack extends cdk.Stack {
  public readonly userPool: cognito.UserPool;
  public readonly userPoolClient: cognito.UserPoolClient;

  constructor(scope: Construct, id: string, props: AuthStackProps) {
    super(scope, id, props);

    const functionName = 'la-metro-post-confirmation';
    const logGroup = new logs.LogGroup(this, 'PostConfirmationFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const postConfirmationFn = new lambda.Function(this, 'PostConfirmationFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'post_confirmation', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      environment: { USERS_TABLE_NAME: props.usersTable.tableName },
      logGroup,
      description: 'Phase 6: seeds the users table on Cognito signup confirmation.',
    });
    props.usersTable.grantWriteData(postConfirmationFn);

    this.userPool = new cognito.UserPool(this, 'UserPool', {
      userPoolName: 'la-metro-users',
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      standardAttributes: { email: { required: true, mutable: true } },
      passwordPolicy: { minLength: 8, requireLowercase: true, requireDigits: true },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      lambdaTriggers: { postConfirmation: postConfirmationFn },
      // Dev convenience: tear down with `cdk destroy`. Production would RETAIN.
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.userPoolClient = this.userPool.addClient('SpaClient', {
      userPoolClientName: 'la-metro-web',
      // Public SPA client: no secret, SRP auth (what Amplify uses).
      generateSecret: false,
      authFlows: { userSrp: true },
      accessTokenValidity: cdk.Duration.hours(1),
      idTokenValidity: cdk.Duration.hours(1),
      refreshTokenValidity: cdk.Duration.days(30),
    });

    new cdk.CfnOutput(this, 'UserPoolId', {
      value: this.userPool.userPoolId,
      description: 'Cognito user pool ID — set as NEXT_PUBLIC_COGNITO_USER_POOL_ID in the frontend.',
    });
    new cdk.CfnOutput(this, 'UserPoolClientId', {
      value: this.userPoolClient.userPoolClientId,
      description: 'Cognito app client ID — set as NEXT_PUBLIC_COGNITO_CLIENT_ID in the frontend.',
    });
  }
}
