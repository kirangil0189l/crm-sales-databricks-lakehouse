# CRM Sales Data Lakehouse using Databricks

## Project Overview

This project demonstrates an end-to-end CRM sales analytics lakehouse using Databricks, PySpark, Delta Lake, and SQL. The project simulates a small business CRM system with customer, product, employee, sales order, and support ticket data. The data is processed through Bronze, Silver, and Gold layers using the lakehouse architecture. Final Gold tables are used for SQL analysis and dashboard reporting.

## Business Problem
A small business wants to analyze customer sales, product performance, regional revenue, and support ticket activity. The goal is to create a clean and reliable analytics platform that can support business reporting and decision-making.

## Tools and Technologies
1.	Databricks
2.	Apache Spark
3.	PySpark
4.	Delta Lake
5.	SQL
6.	Lakehouse Architecture
7.	Databricks SQL Dashboard
8.	GitHub

## Data Model

The project uses five main business tables:

1.	Customers
2.	Products
3.	Employees
4.	Sales Orders
5.	Support Tickets

## Lakehouse Architecture

### Bronze Layer
The Bronze layer stores raw business data.
Tables:

- bronzeCustomers
- bronzeProducts
- bronzeEmployees
- bronzeSalesOrders
- bronzeSupportTickets

### Silver Layer
The Silver layer stores cleaned and standardized data.
Cleaning steps include:
1.	Removing duplicate records
2.	Handling missing values
3.	Converting date columns
4.	Casting numeric fields
5.	Standardizing text fields
6.	Creating calculated fields such as fullName and daysToClose

Tables:

1.	silverCustomers
2.	silverProducts
3.	silverEmployees
4.	silverSalesOrders
5.	silverSupportTickets

### Gold Layer

The Gold layer stores business-ready analytics tables.
Tables:

1.	goldSalesSummary
2.	goldCustomerValue
3.	goldSupportPerformance
4.	goldRegionPerformance

## Key Analysis

The project analyzes:

1.	Total revenue
2.	Completed orders
3.	Active customers
4.	Support ticket volume
5.	Sales by province
6.	Sales by product category
7.	Customer lifetime value
8.	Regional performance
9.	Support tickets by issue type

## Dashboard Preview

![Dashboard Overview](images/dashboardOverview.png)

## Project Workflow

1. Created sample CRM sales data
2. Loaded data into Bronze Delta tables
3. Cleaned and transformed data into Silver tables
4. Created business-ready Gold analytics tables
5. Used SQL to analyze the Gold tables
6. Built a Databricks dashboard for business insights

###Author
Sukhkirandeep Kaur Sidhu

