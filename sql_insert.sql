INSERT INTO groups (name) VALUES (%s);
INSERT INTO teachers (fullname) VALUES (%s);
INSERT INTO subjects (name, teacher_id) VALUES (%s, %s);
INSERT INTO students (fullname, group_id) VALUES (%s, %s) RETURNING id;
INSERT INTO grades (student_id, subject_id, grade, grade_date) VALUES (%s, %s, %s, %s);