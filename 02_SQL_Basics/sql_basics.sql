-- ============================================================================
-- Azure Data Engineer Training: 02. SQL Basics Reference Guide
-- ============================================================================
-- This file contains SQL patterns and scripts useful for relational databases 
-- (Azure SQL, Synapse Dedicated Pools) and Spark SQL (Databricks).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DDL (Data Definition Language) & DML (Data Manipulation Language)
-- ----------------------------------------------------------------------------

-- Create tables
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DepartmentID INT,
    Salary DECIMAL(10, 2),
    HireDate DATE
);

CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(50),
    Location VARCHAR(50)
);

-- Insert sample records (DML)
INSERT INTO Departments (DepartmentID, DepartmentName, Location) VALUES
(10, 'Data Engineering', 'Austin'),
(20, 'Data Science', 'Seattle'),
(30, 'Product Management', 'New York'),
(40, 'DevOps', 'San Francisco');

INSERT INTO Employees (EmployeeID, FirstName, LastName, DepartmentID, Salary, HireDate) VALUES
(101, 'Alex', 'Smith', 10, 95000.00, '2023-01-15'),
(102, 'Emily', 'Jones', 10, 105000.00, '2022-06-11'),
(103, 'Michael', 'Brown', 20, 110000.00, '2021-03-24'),
(104, 'Jessica', 'Davis', 20, 85000.00, '2024-02-10'),
(105, 'David', 'Miller', 30, 90000.00, '2020-11-01'),
(106, 'Sarah', 'Wilson', NULL, 75000.00, '2023-09-18'); -- NULL Department representing a new joiner/unassigned employee


-- ----------------------------------------------------------------------------
-- 2. Basic Filtering, Sorting, & Aggregations
-- ----------------------------------------------------------------------------

-- Filter employees hired after 2022, sorted by salary descending
SELECT EmployeeID, FirstName, LastName, Salary, HireDate
FROM Employees
WHERE HireDate > '2022-01-01'
ORDER BY Salary DESC;

-- Simple aggregation: Average salary, count, max, and min per department
SELECT 
    DepartmentID,
    COUNT(EmployeeID) AS EmployeeCount,
    AVG(Salary) AS AvgSalary,
    MAX(Salary) AS MaxSalary,
    MIN(Salary) AS MinSalary
FROM Employees
GROUP BY DepartmentID
HAVING COUNT(EmployeeID) > 0; -- Filter groups with at least 1 employee


-- ----------------------------------------------------------------------------
-- 3. JOIN Types (Inner, Left, Right, Full Outer)
-- ----------------------------------------------------------------------------

-- INNER JOIN: Employees who belong to a valid department
SELECT e.EmployeeID, e.FirstName, e.LastName, d.DepartmentName
FROM Employees e
INNER JOIN Departments d ON e.DepartmentID = d.DepartmentID;

-- LEFT JOIN: All employees, including those with NO department (e.g. Sarah Wilson)
SELECT e.EmployeeID, e.FirstName, e.LastName, COALESCE(d.DepartmentName, 'Unassigned') AS DepartmentName
FROM Employees e
LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID;

-- RIGHT JOIN: All departments, including those with NO employees (e.g. DevOps)
SELECT d.DepartmentID, d.DepartmentName, e.FirstName, e.LastName
FROM Employees e
RIGHT JOIN Departments d ON e.DepartmentID = d.DepartmentID;

-- FULL OUTER JOIN: All employees and all departments combined
SELECT e.EmployeeID, e.FirstName, e.LastName, d.DepartmentName
FROM Employees e
FULL OUTER JOIN Departments d ON e.DepartmentID = d.DepartmentID;


-- ----------------------------------------------------------------------------
-- 4. Common Table Expressions (CTEs) & Subqueries
-- ----------------------------------------------------------------------------
-- CTEs make complex queries readable and modular compared to nested subqueries.

-- Objective: Find employees whose salary is above the average salary of the whole company.
WITH CompanyAverageSalary AS (
    SELECT AVG(Salary) AS OverallAvgSalary 
    FROM Employees
)
SELECT e.EmployeeID, e.FirstName, e.LastName, e.Salary, c.OverallAvgSalary
FROM Employees e
CROSS JOIN CompanyAverageSalary c
WHERE e.Salary > c.OverallAvgSalary;


-- ----------------------------------------------------------------------------
-- 5. Window Functions (Analytical Functions)
-- ----------------------------------------------------------------------------
-- Unlike GROUP BY, window functions do not collapse rows. They allow calculations 
-- across a set of table rows that are related to the current row.

-- A. ROW_NUMBER(), RANK(), DENSE_RANK()
-- Objective: Rank employees within their department based on salary.
SELECT 
    EmployeeID,
    FirstName,
    LastName,
    DepartmentID,
    Salary,
    ROW_NUMBER() OVER(PARTITION BY DepartmentID ORDER BY Salary DESC) AS RowNum,
    RANK() OVER(PARTITION BY DepartmentID ORDER BY Salary DESC) AS SalaryRank,
    DENSE_RANK() OVER(PARTITION BY DepartmentID ORDER BY Salary DESC) AS SalaryDenseRank
FROM Employees
WHERE DepartmentID IS NOT NULL;

-- B. LEAD() and LAG()
-- Objective: Compare current employee's salary with the next and previous employee's 
-- salary (when sorted by HireDate) to examine salary growth trends.
SELECT 
    EmployeeID,
    FirstName,
    LastName,
    HireDate,
    Salary,
    LAG(Salary, 1, 0) OVER(ORDER BY HireDate ASC) AS PrevHiredEmployeeSalary,
    LEAD(Salary, 1, 0) OVER(ORDER BY HireDate ASC) AS NextHiredEmployeeSalary
FROM Employees;
