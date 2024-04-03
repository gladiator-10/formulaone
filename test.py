# Databricks notebook source
print("Hello World")

# COMMAND ----------

dbutils.secrets.list("formula-one-scope")

# COMMAND ----------

service_credential = dbutils.secrets.get("formula-one-scope","kv-formula-one-service-credential")
application_id = dbutils.secrets.get("formula-one-scope","kv-formula-one-applicaion-id")
directory_id = dbutils.secrets.get("formula-one-scope","kv-formula-one-directory-id")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulaonebwt.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formulaonebwt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formulaonebwt.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formulaonebwt.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formulaonebwt.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@formulaonebwt.dfs.core.windows.net/")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@formulaonebwt.dfs.core.windows.net/",
  mount_point = "/mnt/formula-one",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula-one"))
