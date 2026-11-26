import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";

import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    // Output
    
    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });

    // Integration infrastructure

    // Dead letter Queue
    const dlq = new sqs.Queue(this, "img-dlq", {
      receiveMessageWaitTime: cdk.Duration.seconds(6),
    });

    const imageProcessQueue = new sqs.Queue(this, "img-process-q", {
      receiveMessageWaitTime: cdk.Duration.seconds(6),
      deadLetterQueue: {
        queue: dlq,
        maxReceiveCount: 1,
      },
    });

    // Second Queue
    const mailerQ = new sqs.Queue(this, "mailer-q", {
      receiveMessageWaitTime: cdk.Duration.seconds(6),
    });


    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image topic",
    });

    //table definition
    const imagesTable = new dynamodb.Table(this, "ImagesTable", {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: "name", type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      tableName: "Images",
    });

    


    // Lambda functions

    const persistImageDataFn = new lambdanode.NodejsFunction(
      this,
      "PersistImageDataFn",
      {
        runtime: lambda.Runtime.NODEJS_22_X,
        entry: `${__dirname}/../lambdas/persistImageData.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          TABLE_NAME: imagesTable.tableName,
          BUCKET_NAME: imagesBucket.bucketName,
          REGION: 'eu-west-1'
        },
      }
    );

    // Second lambda func : SES
    const mailerFn = new lambdanode.NodejsFunction(this, "mailer", {
      runtime: lambda.Runtime.NODEJS_22_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(3),
      entry: `${__dirname}/../lambdas/mailer.ts`,
    });

    // Process message in DLQ
    const rejectedFileFn = new lambdanode.NodejsFunction(
      this,
      "RejectedFileFn",
      {
        runtime: lambda.Runtime.NODEJS_22_X,
        entry: `${__dirname}/../lambdas/rejectedFile.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
      }
    );

    // S3 --> SQS
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)
    );

    // SQS --> Lambda
    const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {  // Changed
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });

    // New Event source for new SQS Queue
    const newImageMailEventSource = new events.SqsEventSource(mailerQ, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });

    // DLQ an event source
    const rejectedFileEventSource = new events.SqsEventSource(dlq, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(10),
    });

    // triggers

    newImageTopic.addSubscription(
      new subs.SqsSubscription(imageProcessQueue)
    );

    //Second queue subscribe to SNS topic
    newImageTopic.addSubscription(new subs.SqsSubscription(mailerQ));


    // Event Sources

    // DynamoDB image persistence event calling 
    persistImageDataFn.addEventSource(newImageEventSource);

    // For emailing the associated email address
    mailerFn.addEventSource(newImageMailEventSource);

    // Trigger for DLQ rejectedFileFn
    rejectedFileFn.addEventSource(rejectedFileEventSource);


    // Permissions

    imagesBucket.grantRead(persistImageDataFn);
    imagesTable.grantReadWriteData(persistImageDataFn);

    // Permission to send emails using the SES service

    mailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );


  }
}
