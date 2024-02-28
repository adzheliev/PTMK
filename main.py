import io
import sys
import psycopg2
from datetime import datetime, timedelta
import time
import random
import logging
from config import DB_NAME, DB_USER, DB_PASS, DB_HOST

logging.basicConfig(level=logging.INFO)


class Employee:
    """
    A class representing an employee.

    Args:
        full_name (str): The full name of the employee.
        birth_date (str): The birthdate of the employee in the format YYYY-MM-DD.
        gender (str): The gender of the employee, either "Male" or "Female".

    Attributes:
        id (int): The unique ID of the employee.
        full_name (str): The full name of the employee.
        birth_date (str): The birthdate of the employee in the format YYYY-MM-DD.
        gender (str): The gender of the employee, either "Male" or "Female".

    """

    def __init__(self, full_name, birth_date, gender):
        self.id = None
        self.full_name = full_name
        self.birth_date = birth_date
        self.gender = gender

    def age(self) -> int:
        """
        Calculates the age of the employee.

        Returns:
            int: The age of the employee.

        """
        today = datetime.now()
        birth_date = datetime.strptime(
            self.birth_date,
            '%Y-%m-%d'
        )
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age

    def save(self, db):
        """
        Saves the employee to the database.

        Args:
            db (Database): The database connection object.

        """
        db.add_employee(self)


def generate_random() -> list[Employee]:
    """
    This function generates a list of random Employees.

    Returns:
        list[Employee]: A list of random Employees.
    """
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"
    ]
    first_names = [
        "John", "Jane", "Alex", "Chris", "Pat", "Sam", "Taylor", "Morgan"
    ]
    genders = [
        "Male", "Female"
    ]
    random_employees = []
    for name in range(1000000):
        full_name = f"{random.choice(last_names)} {random.choice(first_names)}"
        birth_date = (datetime.now() - timedelta(days=random.randint(365 * 18, 365 * 65))).strftime('%Y-%m-%d')
        gender = random.choice(genders)
        random_employees.append(Employee(full_name, birth_date, gender))
    return random_employees


def generate_special() -> list[Employee]:
    """
    This function generates a list of 100 random male employees with special last names.

    Returns:
        list[Employee]: A list of 100 random male employees with special last names.
    """
    special_last_names = [
        "Fisher", "Ford", "Fletcher", "Farrell", "Faulkner", "Frost"
    ]
    first_names = [
        "John", "Alex", "Chris", "Pat", "Sam", "Taylor", "Morgan"
    ]
    employees = []

    for _ in range(100):
        full_name = f"{random.choice(special_last_names)} {random.choice(first_names)}"
        birth_date = (datetime.now() - timedelta(days=random.randint(365 * 18, 365 * 65))).strftime('%Y-%m-%d')
        employees.append(Employee(full_name, birth_date, "Male"))
    return employees


class Database:
    """
    A class that represents a database connection.

    Args:
        dbname (str): The name of the database.
        user (str): The username for the database.
        password (str): The password for the database.
        host (str): The hostname of the database.

    Attributes:
        conn (psycopg2.Connection): The database connection object.
        cur (psycopg2.Cursor): The database cursor object.

    """

    def __init__(self, dbname, user, password, host):
        """
        Initializes the database connection.

        Raises:
            psycopg2.Error: If there is an error connecting to the database.

        """
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host
            )
            self.cur = self.conn.cursor()
        except psycopg2.Error as e:
            logging.error(f"Error connecting to DB: {e}")
            sys.exit(1)

    def create_table(self):
        """
        Creates the employees table in the database if it does not already exist.

        Raises:
            psycopg2.Error: If there is an error creating the table.

        """
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                full_name VARCHAR(255),
                birth_date DATE,
                gender VARCHAR(50)
            )
        """)
        self.conn.commit()

    def add_employee(self, employee):
        """
        Adds an employee to the database.

        Args:
            employee (Employee): The employee object to be added.

        Raises:
            psycopg2.Error: If there is an error adding the employee.

        """
        self.cur.execute("""
            INSERT INTO employees (full_name, birth_date, gender)
            VALUES (%s, %s, %s)
        """, (employee.full_name, employee.birth_date, employee.gender))
        self.conn.commit()

    def add_employees_bulk(self, employees):
        """
        Adds a list of employees to the database in bulk.

        Args:
            employees (list[Employee]): The list of employee objects to be added.

        Raises:
            psycopg2.Error: If there is an error adding the employees.

        """
        buffer = io.StringIO()
        for emp in employees:
            buffer.write(f"{emp.full_name},{emp.birth_date},{emp.gender}\n")
        buffer.seek(0)

        try:
            with self.conn.cursor() as cur:
                cur.copy_from(
                    buffer,
                    'employees',
                    sep=',',
                    columns=('full_name', 'birth_date', 'gender')
                )
                self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            logging.error(f"Error during bulk insert with copy_from: {e}")

    def get_all_employees(self):
        """
        Retrieves all employees from the database and prints them to the console.

        Raises:
            psycopg2.Error: If there is an error retrieving the employees.

        """
        self.cur.execute("""
            SELECT DISTINCT full_name, birth_date, gender,
            EXTRACT(YEAR FROM AGE(birth_date)) AS age
            FROM employees
            GROUP BY full_name, birth_date, gender
            ORDER BY full_name, birth_date, age
        """)
        for row in self.cur.fetchall():
            print(
                f"Name: {row[0]}, "
                f"Date of Birth: {row[1]}, "
                f"Gender: {row[2]}, "
                f"Age: {int(row[3])}"
            )

    def get_employees_by_criteria(self, gender, first_letter):
        """
        Retrieves employees from the database based on a given criteria and prints them to the console.

        Args:
            gender (str): The gender of the employees to be retrieved.
            first_letter (str): The first letter of the employees' names to be retrieved.

        Raises:
            psycopg2.Error: If there is an error retrieving the employees.

        """
        start_time = time.time()
        self.cur.execute("""
            SELECT full_name, birth_date, gender
            FROM employees
            WHERE gender = %s AND full_name LIKE %s
            ORDER BY full_name
        """, (gender, first_letter + '%'))
        logging.info(f"Query Time: {time.time() - start_time} seconds")

    def optimize_database(self):
        """
        Optimizes the database by creating indexes on the relevant columns.

        Raises:
            psycopg2.Error: If there is an error optimizing the database.

        """
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_gender ON employees (gender)"
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_full_name ON employees (full_name)"
        )
        self.conn.commit()

    def flush_db(self):
        """
        Flushes the contents of the database.

        Raises:
            psycopg2.Error: If there is an error flushing the database.

        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM employees")
                self.conn.commit()
                logging.info("Database flushed successfully.")
        except psycopg2.Error as e:
            self.conn.rollback()
            logging.error(f"Error flushing database: {e}")

    def close(self):
        """
        Closes the database connection.

        """
        if self.conn:
            self.cur.close()
            self.conn.close()


def main(mode):
    """
    Main function of the application.

    Args:
        mode (str): The mode in which the application is being run. Possible modes are:
            "1": Create the database table.
            "2": Add an employee to the database.
            "3": Retrieve all employees from the database.
            "4": Add a large number of random employees to the database.
            "5": Retrieve male employees whose names start with "F" from the database.
            "6": Optimize the database.
            "flush": Delete all data from the database.
    """
    try:
        db = Database(DB_NAME, DB_USER, DB_PASS, DB_HOST)

        if mode == "1":
            db.create_table()
            logging.info("Table created successfully.")
        elif mode == "2":
            if len(sys.argv) >= 5:
                full_name, birth_date, gender = sys.argv[2], sys.argv[3], sys.argv[4]
                employee = Employee(full_name, birth_date, gender)
                db.add_employee(employee)
                logging.info(f"Employee {full_name} added successfully.")
            else:
                logging.warning(
                    "Not enough arguments provided for adding an employee."
                )
        elif mode == "3":
            db.get_all_employees()
        elif mode == "4":
            bulk_employees = generate_random()
            db.add_employees_bulk(bulk_employees)
            logging.info(
                f"{len(bulk_employees)} employees added successfully."
            )
            special_employees = generate_special()
            db.add_employees_bulk(special_employees)
            logging.info(
                f"{len(special_employees)} employees added successfully."
            )
        elif mode == "5":
            db.get_employees_by_criteria("Male", "F")
        elif mode == "6":
            db.optimize_database()
            logging.info("Database optimized.")
        elif mode == "flush":
            db.flush_db()
        else:
            logging.warning("Invalid mode selected.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.info("Usage: myApp <mode> [arguments]")
    else:
        main(sys.argv[1])
