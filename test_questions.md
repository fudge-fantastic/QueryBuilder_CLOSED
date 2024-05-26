20 complex SQL questions along with their solutions based on the given table structure. 

### 1. Retrieve the average portfolio balance for each age band.
```sql
SELECT age_band, AVG(portfolio_balance) AS average_portfolio_balance
FROM disc_off
GROUP BY age_band;
```

### 2. Find the total online purchase amount for each region.
```sql
SELECT region, SUM(online_purchase_amount) AS total_online_purchase
FROM disc_off
GROUP BY region;
```

### 3. Calculate the average term deposit for self-employed individuals by region.
```sql
SELECT region, AVG(term_deposit) AS average_term_deposit
FROM disc_off
WHERE self_employed = 'Yes'
GROUP BY region;
```

### 4. List the top 5 occupations with the highest average investment in mutual funds.
```sql
SELECT occupation, AVG(investment_in_mutual_fund) AS avg_investment_mutual_fund
FROM disc_off
GROUP BY occupation
ORDER BY avg_investment_mutual_fund DESC
LIMIT 5;
```

### 5. Identify regions where the average home loan is greater than the overall average home loan.
```sql
SELECT region, AVG(home_loan) AS avg_home_loan
FROM disc_off
GROUP BY region
HAVING AVG(home_loan) > (SELECT AVG(home_loan) FROM disc_off);
```

### 6. Find the average credit card transaction amount for each combination of self-employed and occupation.
```sql
SELECT self_employed, occupation, AVG(average_credit_card_transaction) AS avg_credit_card_transaction
FROM disc_off
GROUP BY self_employed, occupation;
```

### 7. Retrieve the top 3 regions with the highest number of personal loan takers.
```sql
SELECT region, COUNT(personal_loan) AS num_personal_loans
FROM disc_off
WHERE personal_loan > 0
GROUP BY region
ORDER BY num_personal_loans DESC
LIMIT 3;
```

### 8. Calculate the sum of investments (mutual fund, tax-saving bond, equity) for each age band.
```sql
SELECT age_band, 
       SUM(investment_in_mutual_fund + investment_tax_saving_bond + investment_in_equity) AS total_investments
FROM disc_off
GROUP BY age_band;
```

### 9. Find the average balance transfer amount for different home statuses.
```sql
SELECT home_status, AVG(balance_transfer) AS avg_balance_transfer
FROM disc_off
GROUP BY home_status;
```

### 10. List the occupations where the average medical insurance is less than the overall average medical insurance.
```sql
SELECT occupation, AVG(medical_insurance) AS avg_medical_insurance
FROM disc_off
GROUP BY occupation
HAVING AVG(medical_insurance) < (SELECT AVG(medical_insurance) FROM disc_off);
```

### 11. Retrieve the regions where the percentage of individuals with term deposits is higher than 50%.
```sql
SELECT region, 
       (COUNT(term_deposit) FILTER (WHERE term_deposit > 0)::DECIMAL / COUNT(*)) * 100 AS percentage_with_term_deposit
FROM disc_off
GROUP BY region
HAVING (COUNT(term_deposit) FILTER (WHERE term_deposit > 0)::DECIMAL / COUNT(*)) * 100 > 50;
```

### 12. Find the average and total investment in commodities for each TV area.
```sql
SELECT tvarea, 
       AVG(investment_in_commudity) AS avg_investment_commodity,
       SUM(investment_in_commudity) AS total_investment_commodity
FROM disc_off
GROUP BY tvarea;
```

### 13. Calculate the average discount offering for each home status and number of children.
```sql
SELECT home_status, children, AVG(discount_offering) AS avg_discount_offering
FROM disc_off
GROUP BY home_status, children;
```

### 14. List the regions where the total investment in derivatives is among the top 3.
```sql
SELECT region, SUM(investment_in_derivative) AS total_investment_derivative
FROM disc_off
GROUP BY region
ORDER BY total_investment_derivative DESC
LIMIT 3;
```

### 15. Find the average portfolio balance and medical insurance for individuals with each status.
```sql
SELECT status, AVG(portfolio_balance) AS avg_portfolio_balance, AVG(medical_insurance) AS avg_medical_insurance
FROM disc_off
GROUP BY status;
```

### 16. Retrieve the top 5 post areas with the highest average personal loan amounts.
```sql
SELECT post_area, AVG(personal_loan) AS avg_personal_loan
FROM disc_off
GROUP BY post_area
ORDER BY avg_personal_loan DESC
LIMIT 5;
```

### 17. Find the regions with the highest average investment in equity for each self-employed status.
```sql
SELECT self_employed, region, AVG(investment_in_equity) AS avg_investment_equity
FROM disc_off
GROUP BY self_employed, region
ORDER BY self_employed, avg_investment_equity DESC
LIMIT 1;
```

### 18. Calculate the average online purchase amount for each gender and occupation combination.
```sql
SELECT gender, occupation, AVG(online_purchase_amount) AS avg_online_purchase
FROM disc_off
GROUP BY gender, occupation;
```

### 19. List the top 3 TV areas with the highest average investment in tax-saving bonds.
```sql
SELECT tvarea, AVG(investment_tax_saving_bond) AS avg_tax_saving_bond
FROM disc_off
GROUP BY tvarea
ORDER BY avg_tax_saving_bond DESC
LIMIT 3;
```

### 20. Find the total term deposits for each combination of self-employed and self-employed partner statuses.
```sql
SELECT self_employed, self_employed_partner, SUM(term_deposit) AS total_term_deposit
FROM disc_off
GROUP BY self_employed, self_employed_partner;
```