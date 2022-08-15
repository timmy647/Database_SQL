-- comp3311 22T1 Assignment 1

-- Q1
create or replace view student_num_program
as
	SELECT student, count(distinct program) 
	FROM program_enrolments
	GROUP BY student
;

drop view if exists Q1;
create or replace view Q1(unswid, name)
as
	SELECT p.unswid, p.name
	FROM student_num_program s
	JOIN people p on (s.student=p.id)
	WHERE count > 4;
;


-- Q2
create or replace view course_tutor_count(unswid, name, course_cnt)
as
	SELECT p.unswid, p.name, count(sr.name)
	FROM course_staff cs
	JOIN people p on (p.id=cs.staff)
	JOIN staff_roles sr on (sr.id=cs.role)
	WHERE sr.name = 'Course Tutor'
	GROUP BY p.id
;

drop view if exists Q2;
create or replace view Q2(unswid, name, course_cnt)
as
	SELECT * FROM course_tutor_count
	WHERE course_cnt = (SELECT MAX(course_cnt) FROM course_tutor_count)
;


-- Q3
drop view if exists Q3;
create or replace view Q3(unswid, name)
as
	SELECT distinct p.unswid, p.name
	FROM course_enrolments ce
	JOIN students s on (ce.student=s.id)
	JOIN people p on (p.id=s.id)
	JOIN courses c on (ce.course=c.id)
	JOIN subjects subj on (subj.id=c.subject)
	WHERE subj.offeredby = (SELECT id FROM orgunits WHERE name = 'School of Law')
	AND s.stype = 'intl'
	AND ce.mark > 85
;


-- Q4
create or replace view local_student_term_course_code(unswid, name, term, code)
as
	SELECT p.unswid, p.name, c.term, subj.code
	FROM course_enrolments ce
	JOIN students s on (s.id=ce.student)
	JOIN people p on (p.id=ce.student)
	JOIN courses c on (c.id=ce.course)
	JOIN subjects subj on (subj.id=c.subject)
	WHERE s.stype = 'local'
;

drop view if exists Q4;
create or replace view Q4(unswid, name)
as
	SELECT c1.unswid, c1.name
	FROM local_student_term_course_code c1
	JOIN local_student_term_course_code c2 on (c1.unswid=c2.unswid)
	WHERE c1.code = 'COMP9020'
	AND c2.code = 'COMP9331'
	AND c1.term = c2.term
;

-- Q5a
create or replace view course_term_mark(id, term, year, mark, course_count)
as
	SELECT c.id, t.name, t.year, ce.mark,
	count(ce.mark) OVER (PARTITION BY c.term)
	FROM courses c
	JOIN terms t on (t.id=c.term)
	JOIN course_enrolments ce on (ce.course=c.id)
	JOIN subjects subj on (subj.id=c.subject)
	WHERE ce.mark IS NOT NULL
	AND subj.code = 'COMP3311'
;

create or replace view Q5a_term_fail_rate(term, fail_rate)
as
	SELECT ctm.term, (count(ctm.mark)/AVG(ctm.course_count))::NUMERIC(10,4)
	FROM course_term_mark ctm
	JOIN courses c on (c.id=ctm.id)
	JOIN subjects subj on (subj.id=c.subject)
	WHERE ctm.year BETWEEN 2009 AND 2012
	AND subj.code = 'COMP3311'
	AND ctm.mark < 50
	GROUP BY ctm.term
;

drop view if exists Q5a;
create or replace view Q5a(term, min_fail_rate)
as
	SELECT t.name, fail_rate
	FROM Q5a_term_fail_rate tfr
	JOIN terms t on (t.name=tfr.term)
	WHERE fail_rate = (SELECT min(fail_rate)
						FROM Q5a_term_fail_rate tfr)
;

-- Q5b
create or replace view Q5b_term_fail_rate(term, fail_rate)
as
	SELECT ctm.term, (count(ctm.mark)/AVG(ctm.course_count))::NUMERIC(10,4)
	FROM course_term_mark ctm
	JOIN courses c on (c.id=ctm.id)
	JOIN subjects subj on (subj.id=c.subject)
	WHERE ctm.year BETWEEN 2016 AND 2019
	AND subj.code = 'COMP3311'
	AND ctm.mark < 50
	GROUP BY ctm.term
;

drop view if exists Q5b;
create or replace view Q5b(term, min_fail_rate)
as
	SELECT t.name, fail_rate
	FROM Q5b_term_fail_rate tfr
	JOIN terms t on (t.name=tfr.term)
	WHERE fail_rate = (SELECT min(fail_rate)
						FROM Q5b_term_fail_rate tfr)
;


-- Q6
drop function if exists Q6;
create or replace function 
	Q6(id integer,code text) returns integer
as $$
	SELECT ce.mark 
	FROM course_enrolments ce
	JOIN courses c on (c.id=ce.course)
	JOIN subjects subj on (subj.id=c.subject)
	WHERE ce.student = $1
	AND subj.code = $2
$$ language sql;


-- Q7
drop function if exists Q7;
create or replace function 
	Q7(year integer, session text) returns table (code text)
as $$
	SELECT subj.code
	FROM courses c
	JOIN subjects subj on (subj.id=c.subject)
	JOIN terms t on (t.id=c.term)
	WHERE t.year = $1
	AND t.session = $2
	AND subj.code like 'COMP%'
	AND subj.career = 'PG'
$$ language sql;



-- Q8
drop function if exists Q8;
create or replace function
	Q8(zid integer) returns setof TermTranscriptRecord
as $$
declare
	r 				record;
	r2				record;
	termcode		char(4);
	mark 			integer;
	uoc  			integer;
	sum_mark		integer;
	num_mark 		integer;
	sum_uoc 		integer;
	overall_wam 	integer;
	-- TermTranscriptRecords record;
	-- transcript TermTranscriptRecord %rowtype;
	
begin
	DROP TABLE IF EXISTS TermTranscriptRecords;
	CREATE TABLE TermTranscriptRecords (
		term 		char(4),
        termwam  	integer, 
        termuocpassed   integer
	);
	IF (SELECT unswid FROM people
		WHERE unswid=zid) IS NULL THEN
		RETURN QUERY SELECT * FROM TermTranscriptRecords;
	ELSE
		sum_mark := 0;
		sum_uoc := 0;
		num_mark := 0;
		FOR r IN 
			SELECT t.id, CAST(termName(t.id) AS char(4)) as t,
			SUM(ce.mark*subj.uoc) as sum_mark,
			SUM(subj.uoc) as sum_uoc
			FROM course_enrolments ce
			JOIN courses c on (c.id=ce.course)
			JOIN subjects subj on (subj.id=c.subject)
			JOIN terms t on (t.id=c.term)
			JOIN people p on (p.id=ce.student)
			WHERE p.unswid = zid
			GROUP BY t.id
		LOOP
			termcode := r.t;
			mark := ROUND(r.sum_mark/(r.sum_uoc)::NUMERIC(5,2));
			IF r.sum_mark IS NOT NULL THEN
				sum_mark = sum_mark + r.sum_mark;
				IF r.sum_uoc IS NOT NULL THEN
					num_mark = num_mark + r.sum_uoc;
				END IF;
			END IF;
			SELECT SUM(subj.uoc) INTO uoc
			FROM course_enrolments ce
			JOIN courses c on (c.id=ce.course)
			JOIN subjects subj on (subj.id=c.subject)
			JOIN terms t on (t.id=c.term)
			JOIN people p on (p.id=ce.student)
			WHERE p.unswid = zid
			AND CAST(termName(t.id) AS char(4)) = termcode
			AND ce.grade in ('SY', 'PT', 'PC', 'PS', 'CR', 'DN', 
							'HD', 'A', 'B', 'C', 'XE', 'T', 'PE',
							'RC', 'RS')
			GROUP BY t.id;
			IF uoc IS NOT NULL THEN
				sum_uoc = sum_uoc + uoc;
			END IF;
		
			INSERT INTO TermTranscriptRecords(term,termwam, termuocpassed) 
			VALUES (termcode, mark, uoc);
		END LOOP;
		
		IF num_mark = 0 THEN
			overall_wam = NULL;
		ELSE 
			overall_wam = ROUND(sum_mark/num_mark::NUMERIC(5,2));
		END IF;
		
		INSERT INTO TermTranscriptRecords(term,termwam, termuocpassed) 
		VALUES ('OVAL', overall_wam, sum_uoc);

		RETURN QUERY SELECT * FROM TermTranscriptRecords;
	END IF;
end
$$ language plpgsql;


-- Q9
drop function if exists Q9;
create or replace function 
	Q9(gid integer) returns setof AcObjRecord
as $$
declare
	r 		record;
	e 		record;
	c	 	text;
	aco 	AcObjRecord;
	i 		integer;
	p 		text;
begin
	FOR r IN
		SELECT id, gtype, gdefby, definition, negated
		FROM acad_object_groups
		WHERE id = gid
		OR parent = gid
	LOOP
		IF r.gdefby = 'enumerated' AND  r.negated = false THEN
			IF r.gtype = 'program' THEN
				FOR e IN
					SELECT p.code
					FROM program_group_members pgm
					JOIN programs p on (p.id=pgm.program)
					WHERE ao_group = r.id
				LOOP
					aco.objtype = r.gtype;
					aco.objcode = CAST(e.code AS TEXT);
					RETURN NEXT aco;
				END LOOP;
			ELSIF r.gtype = 'stream' THEN
				FOR e IN
					SELECT s.code
					FROM stream_group_members pgm
					JOIN streams s on (s.id=pgm.stream)
					WHERE ao_group = r.id
				LOOP
					aco.objtype = r.gtype;
					aco.objcode = CAST(e.code AS TEXT);
					RETURN NEXT aco;
				END LOOP;
			ELSIF r.gtype = 'subject' THEN
				FOR e IN
					SELECT s.code
					FROM subject_group_members pgm
					JOIN subjects s on (s.id=pgm.subject)
					WHERE ao_group = r.id
				LOOP
					aco.objtype = r.gtype;
					aco.objcode = CAST(e.code AS TEXT);
					RETURN NEXT aco;
				END LOOP;
			END IF;
		ELSIF r.gdefby = 'pattern' AND  r.negated = false THEN
			i := 1;
			WHILE (SPLIT_PART(r.definition,',',i) <> '') LOOP
				p := SPLIT_PART(r.definition,',',i);
				IF p SIMILAR TO '{%}' THEN
					p := REPLACE(p,'{','(');
					p := REPLACE(p,'}',')');
					p := REPLACE(p,';','|');
				END IF;
				IF p SIMILAR TO '%#%' THEN
					p := REPLACE(p,'#','_');
				END IF;
				IF p SIMILAR TO '#FREE#'
				OR p SIMILAR TO '#GEN#'
				OR p SIMILAR TO '#F=#' THEN
					p := '';
				END IF;
				IF r.gtype = 'program' THEN
					FOR e IN
						SELECT code FROM programs
						WHERE code SIMILAR TO p
					LOOP
						aco.objtype = r.gtype;
						aco.objcode = CAST(e.code AS TEXT);
						RETURN NEXT aco;
					END LOOP;
				ELSIF r.gtype = 'stream' THEN
					FOR e IN
						SELECT code FROM streams
						WHERE code SIMILAR TO p
					LOOP
						aco.objtype = r.gtype;
						aco.objcode = CAST(e.code AS TEXT);
						RETURN NEXT aco;
					END LOOP;
				ELSIF r.gtype = 'subject' THEN
					FOR e IN
						SELECT code FROM subjects
						WHERE code SIMILAR TO p
					LOOP
						aco.objtype = r.gtype;
						aco.objcode = CAST(e.code AS TEXT);
						RETURN NEXT aco;
					END LOOP;
				END IF;
				i := i+1;
			END LOOP;
		ELSIF r.gdefby = 'query' or r.negated = false THEN
			
		END IF;
	END LOOP;
	
end
$$ language plpgsql;


-- Q10
drop function if exists Q10;
create or replace function
	Q10(codes text) returns setof text
as $$
declare
begin
	RETURN QUERY
	SELECT CAST(subj.code AS TEXT)
	FROM subjects subj
	JOIN subject_prereqs pre on (pre.subject=subj.id)
	JOIN rules r on (r.id=pre.rule)
	JOIN acad_object_groups aog on (aog.id=r.ao_group)
	WHERE aog.gtype = 'subject'
	AND aog.definition LIKE CONCAT('%',codes,'%');
end
$$ language plpgsql;

