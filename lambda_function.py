import json
import os
import boto3
import logging
from typing import Dict, Any
from urllib.parse import unquote_plus
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import S3Event
from aws_lambda_powertools.metrics import Metrics
from lambda_enrichment import EnrichmentProcessor

# Initialize AWS Lambda Powertools
logger = Logger()
tracer = Tracer()
metrics = Metrics()

# Constants for S3 paths
BUCKET_NAME = 'intit-systemsautomations'
RAW_PREFIX = 'SupplierOperations/dataEnrichment/raw/'
ENHANCED_PREFIX = 'SupplierOperations/dataEnrichment/enhanced/'
TEMP_DIR = '/tmp'

class EnrichmentError(Exception):
    """Custom exception for enrichment process errors"""
    pass

@tracer.capture_lambda_handler
@logger.inject_lambda_context
@metrics.log_metrics
def handler(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    """
    Main Lambda handler for the Data Enrichment process
    
    Args:
        event: The event dict from AWS Lambda
        context: The context object from AWS Lambda
        
    Returns:
        Dict containing the processing results
    """
    try:
        # Parse S3 event
        s3_event = S3Event(event)
        bucket = s3_event.bucket_name
        key = unquote_plus(s3_event.object_key)
        
        # Validate event is for raw directory
        if not key.startswith(RAW_PREFIX):
            logger.info(f"Ignoring file not in raw directory: {key}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'File not in raw directory, ignoring',
                    'file': key
                })
            }
        
        # Extract filename and setup paths
        filename = os.path.basename(key)
        input_path = os.path.join(TEMP_DIR, filename)
        output_filename = filename.replace('.csv', '_enhanced.csv')
        output_path = os.path.join(TEMP_DIR, output_filename)
        enhanced_key = f"{ENHANCED_PREFIX}{output_filename}"
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Download input file
        logger.info(f"Downloading file {key} from bucket {bucket}")
        s3_client.download_file(bucket, key, input_path)
        
        # Process the file
        logger.info("Starting enrichment process")
        processor = EnrichmentProcessor(input_path, output_path)
        processor.process()
        
        # Upload processed file
        logger.info(f"Uploading enhanced file to {enhanced_key}")
        s3_client.upload_file(output_path, bucket, enhanced_key)
        
        # Cleanup temporary files
        os.remove(input_path)
        os.remove(output_path)
        
        metrics.add_metric(name="FilesProcessed", unit="Count", value=1)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'File processed successfully',
                'input_file': key,
                'output_file': enhanced_key
            })
        }
        
    except Exception as e:
        logger.exception("Error processing file")
        metrics.add_metric(name="ProcessingErrors", unit="Count", value=1)
        raise EnrichmentError(f"Failed to process file: {str(e)}") 