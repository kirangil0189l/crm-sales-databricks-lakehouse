from pyspark.sql import functions as F
# Bronze Layer
# Project: CRM Sales Data Lakehouse using Databricks

spark.sql("CREATE SCHEMA IF NOT EXISTS crmSalesProject")
spark.sql("USE crmSalesProject")

# Customers data


customers = [
    (1, "Aman", "Gill", "aman.gill@email.com", "416-111-1001", "Toronto", "Ontario", "Retail", "2024-01-05"),
    (2, "Sarah", "Khan", "sarah.khan@email.com", "416-111-1002", "Mississauga", "Ontario", "Corporate", "2024-01-12"),
    (3, "Michael", "Brown", "michael.brown@email.com", "403-111-1003", "Calgary", "Alberta", "Retail", "2024-02-01"),
    (4, "Priya", "Sharma", "priya.sharma@email.com", "587-111-1004", "Edmonton", "Alberta", "Small Business", "2024-02-10"),
    (5, "David", "Lee", "david.lee@email.com", "604-111-1005", "Vancouver", "British Columbia", "Corporate", "2024-03-03")
]

customersCols = [
    "customerId", "firstName", "lastName", "email", "phone",
    "city", "province", "customerSegment", "createdDate"
]

dfCustomers = spark.createDataFrame(customers, customersCols)

# Products data

products = [
    (101, "Cloud Backup Plan", "Cloud Services", 199.99),
    (102, "CRM Starter Package", "Software", 499.99),
    (103, "Data Analytics Setup", "Consulting", 1200.00),
    (104, "Security Audit", "Cybersecurity", 900.00),
    (105, "Power BI Dashboard", "Analytics", 750.00)
]

productsCols = ["productId", "productName", "category", "unitPrice"]

dfProducts = spark.createDataFrame(products, productsCols)

# ----------------------------
# Employees data
# ----------------------------

employees = [
    (201, "John Miller", "Sales", "Ontario"),
    (202, "Neha Patel", "Sales", "Alberta"),
    (203, "Robert Chen", "Support", "British Columbia"),
    (204, "Lisa White", "Support", "Ontario")
]

employeesCols = ["employeeId", "employeeName", "department", "region"]

dfEmployees = spark.createDataFrame(employees, employeesCols)

# Sales orders data

salesOrders = [
    (1001, 1, 101, 201, "2024-03-01", 2, 399.98, "Completed"),
    (1002, 2, 103, 201, "2024-03-05", 1, 1200.00, "Completed"),
    (1003, 3, 102, 202, "2024-03-10", 1, 499.99, "Completed"),
    (1004, 4, 104, 202, "2024-03-15", 1, 900.00, "Pending"),
    (1005, 5, 105, 203, "2024-03-20", 2, 1500.00, "Completed"),
    (1006, 2, 105, 201, "2024-04-01", 1, 750.00, "Completed"),
    (1007, 3, 103, 202, "2024-04-08", 1, 1200.00, "Cancelled"),
    (1008, 1, 102, 201, "2024-04-12", 3, 1499.97, "Completed")
]

salesOrdersCols = [
    "orderId", "customerId", "productId", "employeeId",
    "orderDate", "quantity", "salesAmount", "orderStatus"
]

dfSalesOrders = spark.createDataFrame(salesOrders, salesOrdersCols)

# Support tickets data

supportTickets = [
    (501, 1, "2024-03-02", "2024-03-04", "Medium", "Closed", "Billing"),
    (502, 2, "2024-03-06", None, "High", "Open", "Technical"),
    (503, 3, "2024-03-11", "2024-03-13", "Low", "Closed", "Account"),
    (504, 4, "2024-03-18", None, "High", "Open", "Service Issue"),
    (505, 5, "2024-03-22", "2024-03-25", "Medium", "Closed", "Technical")
]

supportCols = [
    "ticketId", "customerId", "createdDate", "closedDate",
    "priority", "status", "issueType"
]

dfSupportTickets = spark.createDataFrame(supportTickets, supportCols)

# Save directly as Bronze Delta tables

dfCustomers.write.format("delta").mode("overwrite").saveAsTable("bronzeCustomers")
dfProducts.write.format("delta").mode("overwrite").saveAsTable("bronzeProducts")
dfEmployees.write.format("delta").mode("overwrite").saveAsTable("bronzeEmployees")
dfSalesOrders.write.format("delta").mode("overwrite").saveAsTable("bronzeSalesOrders")
dfSupportTickets.write.format("delta").mode("overwrite").saveAsTable("bronzeSupportTickets")

print("Bronze tables created successfully using camelCase names.")

# Check created tables

spark.sql("SHOW TABLES IN crmSalesProject").show()