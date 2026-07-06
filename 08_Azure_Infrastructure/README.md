# 1-Click Azure Infrastructure Deployment

This folder contains the **Azure Resource Manager (ARM) Template** to deploy the complete resource stack required for the Medallion ETL projects:
1. **Azure Storage Account (ADLS Gen2)** (with Hierarchical Namespace enabled)
2. **Azure Databricks Workspace**
3. **Azure Data Factory**

---

## How to Deploy in 3 Steps:

### Step 1: Deploy Custom Template in Azure Portal
1. Open the [Azure Portal](https://portal.azure.com/).
2. In the top search bar, search for **"Deploy a custom template"** and select it.
3. Click on **"Build your own template in the editor"**.
4. Clear the default JSON and copy-paste the contents of the [azuredeploy.json](azuredeploy.json) file.
5. Click **Save**.
6. On the Custom Deployment form, configure:
   * **Subscription**: Your active Azure Subscription.
   * **Resource Group**: Create a new resource group (e.g. `rg-azure-data-engineering`).
   * **Project Prefix**: A short prefix (e.g., `gatlakiran`) to name your services.
7. Click **Review + create** and then **Create**.
*Azure will provision ADLS Gen2, Databricks, and Data Factory in approximately 2-3 minutes.*

---

### Step 2: Initialize Storage Containers
Once the storage account is deployed:
1. Go to your new Storage Account (it will be named `[prefix]adls[unique-string]`).
2. Select **Containers** under the Data Storage menu.
3. Add 4 new containers:
   * `landing`
   * `bronze`
   * `silver`
   * `gold`

---

### Step 3: Link Databricks & Run Pipelines
1. Open your **Azure Databricks Workspace** from the portal.
2. Link it to your GitHub repository:
   * Settings ➡️ Developer ➡️ Git Integration ➡️ Select GitHub and authenticate.
   * Go to Workspace ➡️ Repos ➡️ Add Repo ➡️ Paste `https://github.com/gatlakiran02/Azure-Data-Engineering.git`.
3. Open your **Azure Data Factory** Studio, configure the linked services, and set up your copy/notebook activities as described in the main [azure_deployment_guide.md](../azure_deployment_guide.md).
