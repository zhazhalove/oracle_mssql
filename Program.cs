using System;
using System.Data;
using System.Data.SqlClient;
using Oracle.DataAccess.Client;

namespace oracle_mssql
{
    class Program
    {
        static void Main(string[] args)
        {
            // Define Microsoft SQL Server connection parameters
            string sqlServerInstance = @"SERVER\INSTANCE";
            string sqlDatabase = "DATABASE";
            string sqlConnectionString = string.Format("Server={0};Database={1};Integrated Security=True;TrustServerCertificate=True;", sqlServerInstance, sqlDatabase);

            // Oracle connection details
            string oracleUsername = "";
            string oraclePassword = "";
            string tnsAlias = "TNS ALIAS";  // This should match the TNS alias defined in your tnsnames.ora file

            string oracleConnectionString = string.Format("User Id={0};Password={1};Data Source={2}", oracleUsername, oraclePassword, tnsAlias);

            DataTable resultSet = new DataTable();

            using (OracleConnection oracleConnection = new OracleConnection(oracleConnectionString))
            {
                try
                {
                    oracleConnection.Open();

                    // Define the query to retrieve data from Oracle
                    string queryStatement = @"
SELECT e.employee_id, e.first_name, e.last_name, e.email, e.phone_number, e.hire_date, e.job_id, e.salary,
        e.commission_pct, e.department_id, d.department_name, j.job_title, j.min_salary, j.max_salary
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id
INNER JOIN jobs j ON e.job_id = j.job_id
";

                    // Create and configure the Oracle command
                    OracleCommand oracleCommand = oracleConnection.CreateCommand();
                    oracleCommand.CommandText = queryStatement;
                    oracleCommand.CommandTimeout = 3600; // Seconds

                    // Create a data adapter and fill the DataTable
                    OracleDataAdapter dataAdapter = new OracleDataAdapter(oracleCommand);
                    dataAdapter.Fill(resultSet);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Error fetching data from Oracle: " + ex.Message);
                    return;
                }
            }

            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                foreach (DataRow row in resultSet.Rows)
                {
                    int employeeId = Convert.ToInt32(row["employee_id"]);

                    SqlTransaction transaction = sqlConnection.BeginTransaction(IsolationLevel.ReadCommitted);

                    try
                    {
                        string mergeQuery = @"
MERGE INTO dbo.combined_employees AS target
USING (SELECT 
    @employee_id AS employee_id,
    @first_name AS first_name,
    @last_name AS last_name,
    @email AS email,
    @phone_number AS phone_number,
    @hire_date AS hire_date,
    @job_id AS job_id,
    @salary AS salary,
    @commission_pct AS commission_pct,
    @department_id AS department_id,
    @department_name AS department_name,
    @job_title AS job_title,
    @min_salary AS min_salary,
    @max_salary AS max_salary
) AS source
ON target.employee_id = source.employee_id
WHEN MATCHED THEN
    UPDATE SET 
        first_name = source.first_name,
        last_name = source.last_name,
        email = source.email,
        phone_number = source.phone_number,
        hire_date = source.hire_date,
        job_id = source.job_id,
        salary = source.salary,
        commission_pct = source.commission_pct,
        department_id = source.department_id,
        department_name = source.department_name,
        job_title = source.job_title,
        min_salary = source.min_salary,
        max_salary = source.max_salary
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, department_id, department_name, job_title, min_salary, max_salary)
    VALUES (source.employee_id, source.first_name, source.last_name, source.email, source.phone_number, source.hire_date, source.job_id, source.salary, source.commission_pct, source.department_id, source.department_name, source.job_title, source.min_salary, source.max_salary);
";

                        SqlCommand sqlCommand = sqlConnection.CreateCommand();
                        sqlCommand.CommandText = mergeQuery;
                        sqlCommand.Transaction = transaction;

                        // Add parameters directly using the built-in method
                        sqlCommand.Parameters.Add(new SqlParameter("@employee_id", SqlDbType.Decimal) { Value = CheckDbNull(employeeId) });
                        sqlCommand.Parameters.Add(new SqlParameter("@first_name", SqlDbType.NVarChar, 20) { Value = CheckDbNull(row["first_name"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@last_name", SqlDbType.NVarChar, 25) { Value = CheckDbNull(row["last_name"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@email", SqlDbType.NVarChar, 25) { Value = CheckDbNull(row["email"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@phone_number", SqlDbType.NVarChar, 20) { Value = CheckDbNull(row["phone_number"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@hire_date", SqlDbType.DateTime) { Value = CheckDbNull(row["hire_date"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@job_id", SqlDbType.NVarChar, 10) { Value = CheckDbNull(row["job_id"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@salary", SqlDbType.Decimal) { Value = CheckDbNull(row["salary"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@commission_pct", SqlDbType.Decimal) { Value = CheckDbNull(row["commission_pct"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@department_id", SqlDbType.Decimal) { Value = CheckDbNull(row["department_id"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@department_name", SqlDbType.NVarChar, 30) { Value = CheckDbNull(row["department_name"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@job_title", SqlDbType.NVarChar, 35) { Value = CheckDbNull(row["job_title"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@min_salary", SqlDbType.Decimal) { Value = CheckDbNull(row["min_salary"]) });
                        sqlCommand.Parameters.Add(new SqlParameter("@max_salary", SqlDbType.Decimal) { Value = CheckDbNull(row["max_salary"]) });

                        Console.WriteLine("Executing MERGE Statement for Employee ID: " + employeeId);
                        sqlCommand.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("Error executing SQL command for Employee ID " + employeeId + " : " + ex.Message);
                        transaction.Rollback();
                    }
                }
            }
        }

        static object CheckDbNull(object value)
        {
            return value == null || string.IsNullOrEmpty(value.ToString()) ? DBNull.Value : value;
        }
    }
}
