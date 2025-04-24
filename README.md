# Data Enrichment Tool - AWS Lambda Version

This is the AWS Lambda version of the Data Enrichment Tool, which processes CSV files using the ZoomInfo API to enrich company and contact information.

## Architecture

- **AWS Lambda Function**: Triggered by S3 file uploads
- **Amazon S3**: Stores input and output files
- **AWS Secrets Manager**: Stores ZoomInfo API credentials
- **Amazon CloudWatch**: Monitors function execution and logs
- **AWS X-Ray**: Provides distributed tracing

## Prerequisites

1. AWS CLI installed and configured
2. AWS SAM CLI installed
3. Python 3.12 or later
4. ZoomInfo API credentials stored in AWS Secrets Manager

## Directory Structure

```
.
├── lambda_function.py     # Main Lambda handler
├── lambda_auth.py        # Authentication module for Lambda
├── requirements.txt      # Python dependencies
├── template.yaml        # AWS SAM template
└── README.md           # This file
```

## Setup

1. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. Store ZoomInfo credentials in AWS Secrets Manager:
```bash
aws secretsmanager create-secret \
    --name zoominfo/credentials \
    --secret-string '{"username":"YOUR_USERNAME","password":"YOUR_PASSWORD"}'
```

## Deployment

1. Build the SAM application:
```bash
sam build
```

2. Deploy to AWS:
```bash
sam deploy --guided
```

During the guided deployment, you'll need to:
- Choose a stack name
- Select an AWS Region
- Confirm the changes before deployment

## Usage

1. Upload CSV files to:
```
s3://intit-systemsautomations/SupplierOperations/dataEnrichment/raw/
```

2. The Lambda function will automatically process the file and save the enriched version to:
```
s3://intit-systemsautomations/SupplierOperations/dataEnrichment/enhanced/
```

## Monitoring

- View Lambda function logs in CloudWatch Logs
- Monitor execution metrics in CloudWatch Metrics
- Track function performance in X-Ray
- Check CloudWatch Alarms for error notifications

## Error Handling

The function implements comprehensive error handling:
- Input validation
- API authentication errors
- Processing failures
- S3 operation errors

All errors are logged to CloudWatch Logs and will trigger CloudWatch Alarms if they exceed thresholds.

## Security

- Uses AWS Secrets Manager for credential storage
- Implements least-privilege IAM roles
- Enables AWS X-Ray for security auditing
- Logs all operations to CloudWatch

## Contributing

1. Create a feature branch
2. Make your changes
3. Submit a pull request

## License

Proprietary - All rights reserved
