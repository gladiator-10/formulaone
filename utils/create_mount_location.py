# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Mounting data to ADB

# COMMAND ----------

dbutils.widgets.text("Enter layer name","")
layer_name = dbutils.widgets.get("Enter layer name")

# COMMAND ----------

service_credential = dbutils.secrets.get("formula-one-scope","kv-formula-one-service-credential")
application_id = dbutils.secrets.get("formula-one-scope","kv-formula-one-applicaion-id")
directory_id = dbutils.secrets.get("formula-one-scope","kv-formula-one-directory-id")

# COMMAND ----------

# Check if mount point exist if not create mount point

if not any (mount.mountPoint == f"/mnt/{layer_name}" for mount in dbutils.fs.mounts()):
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}


    dbutils.fs.mount(
    source = "abfss://bronze@formulaonebwt.dfs.core.windows.net/",
    mount_point = f"/mnt/{layer_name}",
    extra_configs = configs)

    print(f"Mount Point '/mnt/{layer_name}' Created successfully")

else:
    print(f"Mount Point '/mnt/{layer_name}' already exist")
