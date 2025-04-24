# Data Enrichment Tool Deployment Script

param(
    [Parameter(Mandatory=$true)]
    [string]$Environment,
    
    [Parameter(Mandatory=$false)]
    [string]$AlertEmail = "",
    
    [Parameter(Mandatory=$false)]
    [int]$RetentionDays = 30,
    
    [Parameter(Mandatory=$false)]
    [int]$MaxConcurrency = 5,
    
    [Parameter(Mandatory=$false)]
    [string]$StackName = "data-enrichment-tool",
    
    [Parameter(Mandatory=$false)]
    [string]$Region = "us-east-1"
)

# Validate environment
if ($Environment -notin @("dev", "prod")) {
    Write-Error "Environment must be either 'dev' or 'prod'"
    exit 1
}

# Ensure AWS CLI is installed and configured
try {
    aws --version
} catch {
    Write-Error "AWS CLI is not installed or not in PATH"
    exit 1
}

# Ensure SAM CLI is installed
try {
    sam --version
} catch {
    Write-Error "AWS SAM CLI is not installed or not in PATH"
    exit 1
}

# Create virtual environment and install dependencies
Write-Host "Creating virtual environment and installing dependencies..."
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Build the SAM application
Write-Host "Building SAM application..."
sam build

# Deploy the stack
Write-Host "Deploying stack to $Environment environment..."
$deployArgs = @(
    "deploy",
    "--stack-name", "$StackName-$Environment",
    "--region", $Region,
    "--capabilities", "CAPABILITY_IAM",
    "--parameter-overrides",
        "Environment=$Environment",
        "RetentionDays=$RetentionDays",
        "MaxConcurrency=$MaxConcurrency"
)

if ($AlertEmail) {
    $deployArgs += "AlertEmail=$AlertEmail"
}

sam @deployArgs

# Verify deployment
Write-Host "Verifying deployment..."
aws cloudformation describe-stacks --stack-name "$StackName-$Environment" --region $Region

Write-Host "Deployment complete! Stack name: $StackName-$Environment"
Write-Host "CloudWatch Dashboard: https://$Region.console.aws.amazon.com/cloudwatch/home?region=$Region#dashboards:name=$StackName-$Environment-dashboard" 