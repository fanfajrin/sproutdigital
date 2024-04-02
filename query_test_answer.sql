WITH MAIN AS (
SELECT
  name
  ,DATE_DIFF(CURRENT_DATE(), hire_date, YEAR) AS haw_laaaang
FROM employee
)
SELECT
  MAIN.name AS EmployeeName
 ,MAIN.haw_laaaang AS YearCnt
FROM MAIN
WHERE MAIN.haw_laaaang > 5
ORDER BY 2 DESC

;

WITH
 EMPL AS (
  SELECT
    department_id
    ,first_salary
  FROM employee
 )
, DEP AS (
  SELECT 
  id
  ,name
  FROM departments
)
,AVGSLR AS (
  SELECT
    AVG(first_salary) AS avg_fs
  FROM employee
)
,FINAL AS (
SELECT
  DEP.name AS department_name
  ,AVG(EMPL.first_salary) AS dep_avg_fs
FROM DEP
LEFT JOIN EMPL ON DEP.id = EMPL.department_id
GROUP BY 1
)
SELECT * FROM FINAL 
WHERE FINAL.dep_avg_fs > (SELECT avg_fs FROM AVGSLR)
ORDER BY FINAL.department_name DESC

;

WITH DEP AS (
  SELECT
  id
  ,name AS department_name
  FROM departments
)
,EMP AS (
  SELECT
  name AS employee_name
  ,department_id
  ,ROW_NUMBER() OVER (ORDER BY first_salary DESC) AS row_num
  FROM employee
)
,FINAL AS (
  SELECT
  EMP.employee_name
  ,DEP.department_name
  FROM EMP
  LEFT JOIN DEP ON DEP.id = EMP.department_id
  WHERE EMP.row_num > 6
  ORDER BY EMP.row_num ASC
)
SELECT * FROM FINAL

;

WITH 
  SLRY AS (
  SELECT 
  *
  ,RANK() OVER (PARTITION BY employee_id ORDER BY effective_date DESC) AS row_num
  FROM salary

  )
  ,SLRY_FINAL AS (
    SELECT 
    SLRY.employee_id
    ,SLRY.salary AS current_salary
    FROM SLRY
    WHERE row_num = 1
  )
  ,EMP AS (
    SELECT
    id
    ,name
    ,first_salary
    FROM employee
  )
  ,FINAL AS (
    SELECT
    EMP.id AS employee_id
    ,EMP.name AS employee_name
    ,EMP.first_salary AS first_salary
    ,SLRY_FINAL.current_salary AS current_salary
    ,ROUND((1-CAST(EMP.first_salary/SLRY_FINAL.current_salary AS BIGNUMERIC))*100,2) AS INCR_PRCNTG
    FROM EMP 
    LEFT JOIN SLRY_FINAL ON EMP.id = SLRY_FINAL.employee_id
    GROUP BY 1,2,3,4
  )
  SELECT * FROM FINAL
