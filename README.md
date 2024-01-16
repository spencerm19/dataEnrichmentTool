# Data Enrichment Tool

This tool enriches contact and firmographic information using the Zoominfo API. It's designed to streamline the process of updating your database with the latest, most accurate data from Zoominfo.

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Prerequisites

Before you begin, ensure you have the following:
```
Python Interpreter (3.6 or later)
Active Zoominfo Credentials    
```
## Installation

Follow these steps to get your development environment running:

1. Clone the repository
  ```
  git clone https://github.com/AustinMBouchard/dataEnrichmentTool
   ```

Install required dependencies

Open your terminal and run the following commands:

    pip install requests
    pip install PySimpleGUI

This will install the requests library for handling HTTP requests and PySimpleGUI for the application's graphical user interface.

## Usage

To use the application, perform the following steps:

1. Navigate to the application directory in your terminal.

2. Run the application using Python:
  ```
  python main.py
  ```
3. When prompted, enter your Zoominfo credentials.

4. Select the file you would like to enrich. Ensure you are using the provided 'Data Enrichment Template' for compatibility.

## File Format

Make sure your file adheres to the 'Data Enrichment Template' format for successful processing. This format includes CSV UTF-8 encoding and contains the following headers:

[Supplier Company]	[Supplier Street]	[Supplier City]	[Supplier State]	[Supplier Zip Code]	[Supplier Country]	[Supplier First Name]	[Supplier Last Name]	[Supplier Email]	[Supplier Phone]	[Site Name]	[Site ID]	[Additional Contact Info]


### Created by

Austin Bouchard
