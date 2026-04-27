from pyspark.sql import functions as F


# Project: CRM Sales Data Lakehouse using Databricks
# Notebook: 01SilverCleaning
# Purpose: Clean Bronze tables and create Silver Delta tables


spark.sql("USE crmSalesProject")


# Read Bronze tables


bronzeCustomers = spark.table("crmSalesProject.bronzeCustomers")
bronzeProducts = spark.table("crmSalesProject.bronzeProducts")
bronzeEmployees = spark.table("crmSalesProject.bronzeEmployees")
bronzeSalesOrders = spark.table("crmSalesProject.bronzeSalesOrders")
bronzeSupportTickets = spark.table("crmSalesProject.bronzeSupportTickets")

# Clean Customers table

silverCustomers = (
    bronzeCustomers
    .dropDuplicates(["customerId"])
    .dropna(subset=["customerId", "email"])
    .withColumn("createdDate", F.to_date("createdDate"))
    .withColumn("fullName", F.concat_ws(" ", "firstName", "lastName"))
    .withColumn("city", F.initcap(F.col("city")))
    .withColumn("province", F.initcap(F.col("province")))
)

# Clean Products table

silverProducts = (
    bronzeProducts
    .dropDuplicates(["productId"])
    .dropna(subset=["productId", "productName"])
    .withColumn("unitPrice", F.col("unitPrice").cast("double"))
    .withColumn("category", F.initcap(F.col("category")))
)

# Clean Employees table

silverEmployees = (
    bronzeEmployees
    .dropDuplicates(["employeeId"])
    .dropna(subset=["employeeId", "employeeName"])
    .withColumn("department", F.initcap(F.col("department")))
    .withColumn("region", F.initcap(F.col("region")))
)

# Clean Sales Orders table

silverSalesOrders = (
    bronzeSalesOrders
    .dropDuplicates(["orderId"])
    .dropna(subset=["orderId", "customerId", "productId"])
    .withColumn("orderDate", F.to_date("orderDate"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("salesAmount", F.col("salesAmount").cast("double"))
    .withColumn("orderStatus", F.initcap(F.col("orderStatus")))
)

# Clean Support Tickets table

silverSupportTickets = (
    bronzeSupportTickets
    .dropDuplicates(["ticketId"])
    .dropna(subset=["ticketId", "customerId"])
    .withColumn("createdDate", F.to_date("createdDate"))
    .withColumn("closedDate", F.to_date("closedDate"))
    .withColumn("priority", F.initcap(F.col("priority")))
    .withColumn("status", F.initcap(F.col("status")))
    .withColumn("issueType", F.initcap(F.col("issueType")))
    .withColumn("daysToClose", F.datediff(F.col("closedDate"), F.col("createdDate")))
)


# Save Silver Delta tables


silverCustomers.write.format("delta").mode("overwrite").saveAsTable("silverCustomers")
silverProducts.write.format("delta").mode("overwrite").saveAsTable("silverProducts")
silverEmployees.write.format("delta").mode("overwrite").saveAsTable("silverEmployees")
silverSalesOrders.write.format("delta").mode("overwrite").saveAsTable("silverSalesOrders")
silverSupportTickets.write.format("delta").mode("overwrite").saveAsTable("silverSupportTickets")

print("Silver tables created successfully.")

# Check created Silver tables


spark.sql("SHOW TABLES IN crmSalesProject").show()