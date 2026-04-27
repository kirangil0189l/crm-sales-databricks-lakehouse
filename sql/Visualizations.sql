USE crmSalesProject;
SELECT
    ROUND((SELECT SUM(totalSales) FROM crmSalesProject.goldSalesSummary), 2) AS totalRevenue,
    (SELECT SUM(totalOrders) FROM crmSalesProject.goldSalesSummary) AS totalCompletedOrders,
    (SELECT COUNT(*) FROM crmSalesProject.goldCustomerValue) AS totalActiveCustomers,
    (SELECT SUM(totalTickets) FROM crmSalesProject.goldSupportPerformance) AS totalSupportTickets;

--Sales by province
SELECT 
    province,
    ROUND(SUM(totalSales), 2) AS totalProvinceSales
FROM crmSalesProject.goldSalesSummary
GROUP BY province
ORDER BY totalProvinceSales DESC;

--Sales by Product Category
SELECT 
    category,
    ROUND(SUM(totalSales), 2) AS categorySales
FROM crmSalesProject.goldSalesSummary
GROUP BY category
ORDER BY categorySales DESC;

--Top customers by lifetime value
SELECT 
    customerId, fullName,city,province,customerSegment,
    ROUND(customerLifetimeValue, 2) AS customerLifetimeValue,
    totalOrders FROM goldCustomerValue ORDER BY customerLifetimeValue DESC;
  --Support Tickets by issue type
    
SELECT 
    issueType,
    SUM(totalTickets) AS totalTickets
FROM crmSalesProject.goldSupportPerformance
GROUP BY issueType
ORDER BY totalTickets DESC;
