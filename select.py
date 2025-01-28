import logging

from psycopg2 import DatabaseError

from connection import create_connection

def select_data(expression:str, *args):
    try:
        with create_connection() as conn:
            if conn is not None:
                c = conn.cursor()
                try:
                    #logging.error(f"Executing query: {expression} with args: {args}")
                    c.execute(expression, args)
                    rows = c.fetchall()
                    [print(row) for row in rows]
                    print('_'  * 70)
                except DatabaseError as e:
                    logging.error(e)
                finally:
                    c.close()
            else:
                logging.error("Unable to connect to the database.")
    except RuntimeError as e:
        logging.error(e)




sql_expression_01 = """
    SELECT s.fullname AS student_name, AVG(g.grade::INTEGER) AS avg_grade
    FROM students s
    JOIN grades g ON s.id = g.students_id
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT %s;

"""

sql_expression_02 = """
    SELECT s.fullname AS student_name, AVG(g.grade::INTEGER) AS avg_grade
    FROM students s
    JOIN grades g ON s.id = g.students_id
    JOIN subjects subj ON g.subject_id = subj.id
    WHERE subj.name = %s
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 1;


"""

sql_expression_03 = """
    SELECT g.name AS group_name, AVG(gr.grade::INTEGER) AS avg_grade
    FROM groups g
    JOIN students s ON g.id = s.group_id
    JOIN grades gr ON s.id = gr.students_id
    JOIN subjects subj ON gr.subject_id = subj.id
    WHERE subj.name = %s
    GROUP BY g.id;

"""

sql_expression_04 = """
    SELECT AVG(grade::INTEGER) AS avg_grade
    FROM grades;

"""
sql_expression_05 = """
    SELECT subj.name AS course_name
    FROM subjects subj
    JOIN teachers t ON subj.teacher_id = t.id
    WHERE t.fullname = %s

"""
sql_expression_06 = """
    SELECT s.fullname AS student_name
    FROM students s
    JOIN groups g ON s.group_id = g.id
    WHERE g.name = %s
"""
sql_expression_07 = """
    SELECT s.fullname AS student_name, gr.grade, subj.name AS subject_name
    FROM students s
    JOIN groups g ON s.group_id = g.id
    JOIN grades gr ON s.id = gr.students_id
    JOIN subjects subj ON gr.subject_id = subj.id
    WHERE g.name = %s AND subj.name = %s;

"""
sql_expression_08 = """
    SELECT AVG(gr.grade::INTEGER) AS avg_grade
    FROM grades gr
    JOIN subjects subj ON gr.subject_id = subj.id
    JOIN teachers t ON subj.teacher_id = t.id
    WHERE t.fullname = %s

"""
sql_expression_09 = """
    SELECT DISTINCT subj.name AS course_name
    FROM students s
    JOIN grades gr ON s.id = gr.students_id
    JOIN subjects subj ON gr.subject_id = subj.id
    WHERE s.fullname = %s;

"""
sql_expression_10 = """
    SELECT DISTINCT subj.name AS course_name
    FROM students s
    JOIN grades gr ON s.id = gr.students_id
    JOIN subjects subj ON gr.subject_id = subj.id
    JOIN teachers t ON subj.teacher_id = t.id
    WHERE s.fullname = %s AND t.fullname = %s;


"""
if __name__ == '__main__':
    select_data(sql_expression_01, 5)
    select_data(sql_expression_02, 'Поява')
    select_data(sql_expression_03, 'Поява')
    select_data(sql_expression_04)
    select_data(sql_expression_05, 'Гліб Тимченко')
    select_data(sql_expression_06, 'витягувати')
    select_data(sql_expression_07, 'Обуритися', 'Монета')
    select_data(sql_expression_08, 'Гліб Тимченко')
    select_data(sql_expression_09, 'Богдан Петренко')
    select_data(sql_expression_10, 'Теодор Вернидуб', 'Адам Єрмоленко')

