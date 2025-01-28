import logging
from faker import Faker
from psycopg2 import DatabaseError
from random import randint, choice
from connection import create_connection

fake = Faker('uk_UA')

def insert_data(conn, number_of_records: int, sql_expression: str, generate_args):
    c = conn.cursor()
    try:
        results = []
        for i in range(number_of_records):
            args = generate_args(i)
            c.execute(sql_expression, args)
            if sql_expression.strip().startswith("INSERT") and "RETURNING" in sql_expression:
                results.append(c.fetchone()[0])
        conn.commit()
        return results
    except DatabaseError as e:
        logging.error(f"Database error: {e}")
        conn.rollback()
    finally:
        c.close()

if __name__ == '__main__':
    sql_insert_group_data = """
        INSERT INTO groups (name) VALUES (%s) RETURNING id; 
    """

    sql_insert_teachers_data = """
        INSERT INTO teachers (fullname) VALUES (%s) RETURNING id;
    """

    sql_insert_subjects_data = """
        INSERT INTO subjects (name, teacher_id) VALUES (%s, %s) RETURNING id;
    """

    sql_insert_students_data = """
        INSERT INTO students (fullname, group_id) VALUES (%s, %s) RETURNING id;
    """

    sql_insert_grades_data = """
        INSERT INTO grades (students_id, subject_id, grade) VALUES (%s, %s, %s);
    """

    try:
        with create_connection() as conn:
            # 1. Insert groups
            groups = insert_data(
                conn,
                3,  # 3 groups
                sql_insert_group_data,
                lambda i: (fake.word().capitalize(),)
            )

            # 2. Insert teachers
            teachers = insert_data(
                conn,
                randint(3, 5),  # 3-5 teachers
                sql_insert_teachers_data,
                lambda i: (fake.name(),)
            )

            # 3. Insert subjects
            subjects = insert_data(
                conn,
                randint(5, 8),  # 5-8 subjects
                sql_insert_subjects_data,
                lambda i: (fake.word().capitalize(), choice(teachers))
            )

            # 4. Insert students
            students = insert_data(
                conn,
                randint(30, 50),  # 30-50 students
                sql_insert_students_data,
                lambda i: (fake.name(), choice(groups))
            )

            # 5. Insert grades
            for student in students:
                for subject in subjects:
                    num_grades = randint(5, 20)  # Up to 20 grades per student per subject
                    insert_data(
                        conn,
                        num_grades,
                        sql_insert_grades_data,
                        lambda i: (
                            student,  # Student ID
                            subject,  # Subject ID
                            randint(60, 100)  # Random grade (e.g., 60-100)
                        )
                    )

    except RuntimeError as err:
        logging.error(f"Runtime error: {err}")
