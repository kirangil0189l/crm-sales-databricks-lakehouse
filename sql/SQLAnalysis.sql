USE crmSalesProject;
SHOW TABLES;
--Sales Summary
SELECT 
    orderMonth,province, category, totalSales, totalOrders, totalCustomers
FROM goldSalesSummary ORDER BY orderMonth, province, category;

--Total sales by province
SELECT 
    province, ROUND(SUM(totalSales), 2) AS totalProvinceSales,
    SUM(totalOrders) AS totalOrders, SUM(totalCustomers) AS totalCustomers
FROM goldSalesSummary GROUP BY province ORDER BY totalProvinceSales DESC;

--Top customers by lifetime value
SELECT 
    customerId, fullName,city,province,customerSegment,
    ROUND(customerLifetimeValue, 2) AS customerLifetimeValue,
    totalOrders FROM goldCustomerValue ORDER BY customerLifetimeValue DESC;

--Sales by product category
SELECT 
    category, ROUND(SUM(totalSales), 2) AS categorySales,SUM(totalOrders) AS totalOrders
FROM goldSalesSummary GROUP BY category ORDER BY categorySales DESC;

--Regional Sales Performance
SELECT 
    region, department, ROUND(totalSales, 2) AS totalSales, totalOrders FROM goldRegionPerformance
ORDER BY totalSales DESC;

--Support Ticket Analysis
SELECT 
    priority,status,issueType,totalTickets,
    ROUND(avgDaysToClose, 2) AS avgDaysToClose FROM goldSupportPerformance ORDER BY priority, status, issueType;

--Open support tickets by priority
SELECT 
    priority, SUM(totalTickets) AS openTickets FROM goldSupportPerformance WHERE status = 'Open' GROUP BY priority
ORDER BY openTickets DESC;

--KPI SUmmary
SELECT
    ROUND((SELECT SUM(totalSales) FROM goldSalesSummary), 2) AS totalRevenue,
    (SELECT SUM(totalOrders) FROM goldSalesSummary) AS totalCompletedOrders,
    (SELECT COUNT(*) FROM goldCustomerValue) AS totalActiveCustomers,
    (SELECT SUM(totalTickets) FROM goldSupportPerformance) AS totalSupportTickets;