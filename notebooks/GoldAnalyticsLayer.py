from pyspark.sql import functions as F


# Project: CRM Sales Data Lakehouse using Databricks
# Notebook: 02GoldAnalytics
# Purpose: Create business-ready Gold analytics tables

spark.sql("USE crmSalesProject")

# Read Silver tables

silverCustomers = spark.table("crmSalesProject.silverCustomers")
silverProducts = spark.table("crmSalesProject.silverProducts")
silverEmployees = spark.table("crmSalesProject.silverEmployees")
silverSalesOrders = spark.table("crmSalesProject.silverSalesOrders")
silverSupportTickets = spark.table("crmSalesProject.silverSupportTickets")


# Create enriched sales table
# Join sales orders with customers, products, and employees

salesEnriched = (
    silverSalesOrders
    .join(silverCustomers, "customerId", "left")
    .join(silverProducts, "productId", "left")
    .join(silverEmployees, "employeeId", "left")
)

# Gold Table 1: Sales Summary
# Shows total sales, orders, and customers by month, province, and product category

goldSalesSummary = (
    salesEnriched
    .filter(F.col("orderStatus") == "Completed")
    .withColumn("orderMonth", F.date_format("orderDate", "yyyy-MM"))
    .groupBy("orderMonth", "province", "category")
    .agg(
        F.sum("salesAmount").alias("totalSales"),
        F.countDistinct("orderId").alias("totalOrders"),
        F.countDistinct("customerId").alias("totalCustomers")
    )
    .orderBy("orderMonth", "province", "category")
)

# Gold Table 2: Customer Value
# Shows customer lifetime value and total orders

goldCustomerValue = (
    salesEnriched
    .filter(F.col("orderStatus") == "Completed")
    .groupBy("customerId", "fullName", "city", "province", "customerSegment")
    .agg(
        F.sum("salesAmount").alias("customerLifetimeValue"),
        F.countDistinct("orderId").alias("totalOrders")
    )
    .orderBy(F.desc("customerLifetimeValue"))
)

# Gold Table 3: Support Performance
# Shows support ticket volume and average closing time

goldSupportPerformance = (
    silverSupportTickets
    .groupBy("priority", "status", "issueType")
    .agg(
        F.countDistinct("ticketId").alias("totalTickets"),
        F.avg("daysToClose").alias("avgDaysToClose")
    )
    .orderBy("priority", "status", "issueType")
)

# Gold Table 4: Regional Performance
# Shows total sales by employee region and department

goldRegionPerformance = (
    salesEnriched
    .filter(F.col("orderStatus") == "Completed")
    .groupBy("region", "department")
    .agg(
        F.sum("salesAmount").alias("totalSales"),
        F.countDistinct("orderId").alias("totalOrders")
    )
    .orderBy(F.desc("totalSales"))
)

# Save Gold Delta tables

goldSalesSummary.write.format("delta").mode("overwrite").saveAsTable("goldSalesSummary")
goldCustomerValue.write.format("delta").mode("overwrite").saveAsTable("goldCustomerValue")
goldSupportPerformance.write.format("delta").mode("overwrite").saveAsTable("goldSupportPerformance")
goldRegionPerformance.write.format("delta").mode("overwrite").saveAsTable("goldRegionPerformance")

print("Gold analytics tables created successfully.")

# Check created tables

spark.sql("SHOW TABLES IN crmSalesProject").show()