# Sample DuckDB Queries for Hospital Data Analysis

## Basic Data Exploration

### 1. Data Overview
```sql
-- Get total record count
SELECT COUNT(*) as total_records FROM {table};

-- Get data date range
SELECT 
    MIN(discharge_year) as earliest_year,
    MAX(discharge_year) as latest_year,
    COUNT(DISTINCT hospital_county) as unique_counties
FROM {table};

-- Sample data preview
SELECT * FROM {table} LIMIT 10;
```

### 2. Schema Information
```sql
-- Get column information (use via API)
DESCRIBE SELECT * FROM {table};
```

## Hospital Analysis

### 3. Hospital Performance Metrics
```sql
-- Top hospitals by patient volume
SELECT 
    hospital_county,
    COUNT(*) as patient_count,
    AVG(total_charges::FLOAT) as avg_charges,
    AVG(length_of_stay::FLOAT) as avg_length_of_stay
FROM {table}
WHERE total_charges IS NOT NULL 
  AND length_of_stay IS NOT NULL
GROUP BY hospital_county
ORDER BY patient_count DESC
LIMIT 20;
```

### 4. Cost Analysis by Hospital
```sql
-- Hospital cost efficiency analysis
SELECT 
    hospital_county,
    COUNT(*) as cases,
    AVG(total_charges::FLOAT) as avg_cost,
    MEDIAN(total_charges::FLOAT) as median_cost,
    MIN(total_charges::FLOAT) as min_cost,
    MAX(total_charges::FLOAT) as max_cost,
    STDDEV(total_charges::FLOAT) as cost_variance
FROM {table}
WHERE total_charges IS NOT NULL 
  AND total_charges != 'NULL'
  AND total_charges::FLOAT > 0
GROUP BY hospital_county
HAVING COUNT(*) >= 100  -- Only hospitals with significant volume
ORDER BY avg_cost DESC
LIMIT 15;
```

## Diagnosis and Treatment Analysis

### 5. Most Common Diagnoses
```sql
-- Top diagnoses by frequency
SELECT 
    ccs_diagnosis_description,
    COUNT(*) as case_count,
    AVG(total_charges::FLOAT) as avg_cost,
    AVG(length_of_stay::FLOAT) as avg_stay
FROM {table}
WHERE ccs_diagnosis_description IS NOT NULL
  AND total_charges IS NOT NULL
  AND length_of_stay IS NOT NULL
GROUP BY ccs_diagnosis_description
ORDER BY case_count DESC
LIMIT 20;
```

### 6. Most Expensive Diagnoses
```sql
-- Diagnoses with highest average costs
SELECT 
    ccs_diagnosis_description,
    COUNT(*) as case_count,
    AVG(total_charges::FLOAT) as avg_cost,
    MEDIAN(total_charges::FLOAT) as median_cost,
    AVG(length_of_stay::FLOAT) as avg_stay
FROM {table}
WHERE ccs_diagnosis_description IS NOT NULL
  AND total_charges IS NOT NULL
  AND total_charges != 'NULL'
  AND total_charges::FLOAT > 0
GROUP BY ccs_diagnosis_description
HAVING COUNT(*) >= 50  -- Minimum case volume for statistical significance
ORDER BY avg_cost DESC
LIMIT 15;
```

## Demographic Analysis

### 7. Age Group Analysis
```sql
-- Patient demographics and costs by age group
SELECT 
    age_group,
    COUNT(*) as patient_count,
    AVG(total_charges::FLOAT) as avg_charges,
    AVG(length_of_stay::FLOAT) as avg_stay,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM {table}
WHERE age_group IS NOT NULL
  AND total_charges IS NOT NULL
  AND length_of_stay IS NOT NULL
GROUP BY age_group
ORDER BY 
    CASE age_group
        WHEN '0 to 17' THEN 1
        WHEN '18 to 29' THEN 2
        WHEN '30 to 49' THEN 3
        WHEN '50 to 69' THEN 4
        WHEN '70 or Older' THEN 5
        ELSE 6
    END;
```

### 8. Gender-based Analysis
```sql
-- Healthcare utilization by gender
SELECT 
    gender,
    COUNT(*) as cases,
    AVG(total_charges::FLOAT) as avg_charges,
    AVG(length_of_stay::FLOAT) as avg_stay,
    COUNT(CASE WHEN apr_severity_of_illness_description = 'Extreme' THEN 1 END) as extreme_cases
FROM {table}
WHERE gender IS NOT NULL
  AND gender != 'U'  -- Exclude unknown gender
GROUP BY gender
ORDER BY cases DESC;
```

## Severity and Risk Analysis

### 9. Severity Distribution
```sql
-- Distribution of illness severity
SELECT 
    apr_severity_of_illness_description as severity,
    COUNT(*) as case_count,
    AVG(total_charges::FLOAT) as avg_cost,
    AVG(length_of_stay::FLOAT) as avg_stay,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM {table}
WHERE apr_severity_of_illness_description IS NOT NULL
GROUP BY apr_severity_of_illness_description
ORDER BY 
    CASE apr_severity_of_illness_description
        WHEN 'Minor' THEN 1
        WHEN 'Moderate' THEN 2
        WHEN 'Major' THEN 3
        WHEN 'Extreme' THEN 4
        ELSE 5
    END;
```

### 10. Risk of Mortality Analysis
```sql
-- Mortality risk vs costs and length of stay
SELECT 
    apr_risk_of_mortality as mortality_risk,
    COUNT(*) as cases,
    AVG(total_charges::FLOAT) as avg_charges,
    AVG(length_of_stay::FLOAT) as avg_stay,
    COUNT(CASE WHEN length_of_stay::FLOAT > 10 THEN 1 END) as long_stay_cases
FROM {table}
WHERE apr_risk_of_mortality IS NOT NULL
  AND total_charges IS NOT NULL
  AND length_of_stay IS NOT NULL
GROUP BY apr_risk_of_mortality
ORDER BY 
    CASE apr_risk_of_mortality
        WHEN 'Minor' THEN 1
        WHEN 'Moderate' THEN 2
        WHEN 'Major' THEN 3
        WHEN 'Extreme' THEN 4
        ELSE 5
    END;
```

## Payment and Insurance Analysis

### 11. Payment Type Analysis
```sql
-- Analysis by payment method
SELECT 
    type_of_admission,
    payment_typology_1 as primary_payment,
    COUNT(*) as cases,
    AVG(total_charges::FLOAT) as avg_charges,
    MEDIAN(total_charges::FLOAT) as median_charges
FROM {table}
WHERE payment_typology_1 IS NOT NULL
  AND type_of_admission IS NOT NULL
  AND total_charges IS NOT NULL
GROUP BY type_of_admission, payment_typology_1
ORDER BY cases DESC
LIMIT 20;
```

## Procedure Analysis

### 12. Most Common Procedures
```sql
-- Top procedures by volume and cost
SELECT 
    ccs_procedure_description,
    COUNT(*) as procedure_count,
    AVG(total_charges::FLOAT) as avg_cost,
    AVG(length_of_stay::FLOAT) as avg_stay
FROM {table}
WHERE ccs_procedure_description IS NOT NULL
  AND ccs_procedure_description != 'NO PROC'
  AND total_charges IS NOT NULL
GROUP BY ccs_procedure_description
ORDER BY procedure_count DESC
LIMIT 15;
```

## Advanced Analytics

### 13. Cost Outlier Detection
```sql
-- Identify high-cost outlier cases
WITH cost_stats AS (
    SELECT 
        AVG(total_charges::FLOAT) as mean_cost,
        STDDEV(total_charges::FLOAT) as std_cost
    FROM {table}
    WHERE total_charges IS NOT NULL 
      AND total_charges::FLOAT > 0
)
SELECT 
    hospital_county,
    ccs_diagnosis_description,
    total_charges::FLOAT as cost,
    length_of_stay::FLOAT as stay,
    age_group,
    apr_severity_of_illness_description
FROM {table}
CROSS JOIN cost_stats
WHERE total_charges::FLOAT > (mean_cost + 3 * std_cost)  -- 3 sigma outliers
ORDER BY total_charges::FLOAT DESC
LIMIT 20;
```

### 14. Efficiency Score by Hospital
```sql
-- Hospital efficiency scoring
WITH hospital_metrics AS (
    SELECT 
        hospital_county,
        AVG(total_charges::FLOAT) as avg_cost,
        AVG(length_of_stay::FLOAT) as avg_stay,
        COUNT(*) as volume,
        AVG(CASE WHEN apr_severity_of_illness_description = 'Minor' THEN 1 ELSE 0 END) as minor_rate
    FROM {table}
    WHERE total_charges IS NOT NULL 
      AND length_of_stay IS NOT NULL
    GROUP BY hospital_county
    HAVING COUNT(*) >= 100
),
benchmarks AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY avg_cost) as cost_25th,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY avg_stay) as stay_25th
    FROM hospital_metrics
)
SELECT 
    hm.hospital_county,
    hm.volume,
    hm.avg_cost,
    hm.avg_stay,
    CASE 
        WHEN hm.avg_cost <= b.cost_25th AND hm.avg_stay <= b.stay_25th THEN 'High Efficiency'
        WHEN hm.avg_cost <= PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_cost) OVER() THEN 'Good Efficiency'
        ELSE 'Needs Improvement'
    END as efficiency_rating
FROM hospital_metrics hm
CROSS JOIN benchmarks b
ORDER BY hm.avg_cost ASC, hm.avg_stay ASC;
```

### 15. Seasonal Analysis (if date fields available)
```sql
-- Monthly admission patterns
SELECT 
    discharge_year,
    COUNT(*) as admissions,
    AVG(total_charges::FLOAT) as avg_charges,
    AVG(length_of_stay::FLOAT) as avg_stay
FROM {table}
WHERE discharge_year IS NOT NULL
GROUP BY discharge_year
ORDER BY discharge_year;
```

## Data Quality Checks

### 16. Data Completeness Assessment
```sql
-- Check data completeness across key fields
SELECT 
    'total_charges' as field_name,
    COUNT(*) as total_records,
    COUNT(total_charges) as non_null_records,
    COUNT(total_charges) * 100.0 / COUNT(*) as completeness_pct
FROM {table}

UNION ALL

SELECT 
    'length_of_stay' as field_name,
    COUNT(*) as total_records,
    COUNT(length_of_stay) as non_null_records,
    COUNT(length_of_stay) * 100.0 / COUNT(*) as completeness_pct
FROM {table}

UNION ALL

SELECT 
    'hospital_county' as field_name,
    COUNT(*) as total_records,
    COUNT(hospital_county) as non_null_records,
    COUNT(hospital_county) * 100.0 / COUNT(*) as completeness_pct
FROM {table}

ORDER BY completeness_pct DESC;
```

## Usage Instructions

1. **Via Streamlit Interface (http://localhost:8501)**:
   - Copy any query above
   - Replace `{table}` placeholder (it's automatic in the interface)
   - Paste into the "Custom SQL Query" text area
   - Click "Execute Query"

2. **Via REST API (http://localhost:8002)**:
   ```bash
   curl -X POST "http://localhost:8002/query/parquet" \
     -H "Content-Type: application/json" \
     -d '{
       "bucket_name": "hospital-data",
       "file_path": "processed/hospital_data.parquet",
       "query": "YOUR_QUERY_HERE",
       "limit": 1000
     }'
   ```

3. **Data Types**: 
   - Many fields come as strings and may need casting (e.g., `total_charges::FLOAT`)
   - Handle NULL values appropriately in WHERE clauses
   - Use CASE statements for categorical ordering

4. **Performance Tips**:
   - Add LIMIT clauses for large result sets
   - Use specific column selection instead of SELECT *
   - Filter early with WHERE clauses
   - Use HAVING for group-level filtering
