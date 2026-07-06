# Azure Data Factory (ADF) - Reference Guide & JSON Templates

Azure Data Factory is a cloud-based data integration service that allows you to create data-driven workflows for orchestrating and automating data movement and data transformation.

---

## 1. Core ADF Components

To build pipelines, ADF relies on five primary components:
1. **Pipelines**: Logical grouping of activities that perform a task.
2. **Activities**: Steps within a pipeline (e.g., Copy Data, Databricks Notebook, Lookup, Web, Wait).
3. **Datasets**: Named views of data pointing to the locations of data you want to use.
4. **Linked Services**: Define the connection strings/authentication details to external resources (like SQL Database, Blob Storage, ADLS Gen2, Databricks).
5. **Integration Runtime (IR)**: The compute infrastructure used by Azure Data Factory to execute data movement and orchestration (AutoResolveIntegrationRuntime, Self-Hosted IR, Azure-SSIS IR).

---

## 2. Copy Data Activity JSON Template
The Copy Activity executes a data movement. Below is a parameterized Copy Activity JSON definition that copies a CSV file from Azure Blob Storage (Bronze Source) to Azure Data Lake Storage (ADLS) Gen2.

```json
{
    "name": "CopyRawSalesToADLS",
    "properties": {
        "description": "Copies raw sales CSV from Azure Blob Storage to Azure Data Lake Storage Gen2 raw container.",
        "activities": [
            {
                "name": "CopyRawSalesFile",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 3,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "AzureBlobStorageReadSettings",
                            "recursive": true,
                            "wildcardFileName": "*.csv"
                        },
                        "formatSettings": {
                            "type": "DelimitedTextReadSettings"
                        }
                    },
                    "sink": {
                        "type": "DelimitedTextSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings"
                        },
                        "formatSettings": {
                            "type": "DelimitedTextWriteSettings",
                            "quoteChar": "\"",
                            "escapeChar": "\\",
                            "firstRowAsHeader": true
                        }
                    },
                    "enableStaging": false
                },
                "inputs": [
                    {
                        "referenceName": "InputBlobDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "ContainerName": "landing-zone",
                            "DirectoryName": "sales"
                        }
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "OutputADLSDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "FileSystem": "bronze",
                            "Folder": "sales/raw"
                        }
                    }
                ]
            }
        ],
        "parameters": {
            "windowStart": {
                "type": "string"
            }
        },
        "annotations": []
    }
}
```

---

## 3. Azure Databricks (ADB) Notebook Activity JSON Template
In modern Lakehouse architectures, ADF is typically used to orchestrate ingestion, while Azure Databricks performs the heavy transformation. Below is the JSON for a Databricks Notebook Activity that passes the current ADF trigger run time as a parameter to the Databricks notebook.

```json
{
    "name": "OrchestrateMedallionETL",
    "properties": {
        "description": "Triggers the Databricks notebook to transform raw data to Bronze, Silver, and Gold layers.",
        "activities": [
            {
                "name": "RunSilverTransformation",
                "type": "DatabricksNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.02:00:00",
                    "retry": 1,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "notebookPath": "/Shared/Ecommerce_ETL/silver",
                    "baseParameters": {
                        "adf_run_id": "@pipeline().RunId",
                        "execution_date": "@pipeline().parameters.windowStart"
                    }
                },
                "linkedServiceName": {
                    "referenceName": "LinkedService_AzureDatabricks",
                    "type": "LinkedServiceReference"
                }
            }
        ],
        "parameters": {
            "windowStart": {
                "type": "string",
                "defaultValue": "2026-07-06"
            }
        }
    }
}
```

---

## 4. Passing Parameters from ADF to Databricks
To consume these parameters inside your Databricks notebooks (ADB), use **dbutils widgets** in Python:

```python
# 1. Read parameters passed by ADF
dbutils.widgets.text("adf_run_id", "", "ADF Run ID")
dbutils.widgets.text("execution_date", "", "Execution Date")

# 2. Assign parameters to variables
run_id = dbutils.widgets.get("adf_run_id")
exec_date = dbutils.widgets.get("execution_date")

print(f"Executing pipeline for date: {exec_date} (ADF Pipeline Run ID: {run_id})")
```
