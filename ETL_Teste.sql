-- Preparação pré migração
CREATE TABLE "lucas.reports" (
"index" INTEGER,
  "id" TEXT,
  "description" TEXT
)
INSERT INTO lucas.reports (id, description) VALUES (Decimal('1'), 'teste')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('21'), 'leo rhoncus sed vestibulum sit amet cursus')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('22'), 'volutpat quam pede lobortis ligula')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('23'), 'eleifend luctus ultricies eu nibh quisque id justo')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('25'), 'nulla pede ullamcorper augue a')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('26'), 'sit amet justo morbi ut')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('27'), 'mauris vulputate elementum nullam varius nulla')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('29'), 'consequat nulla nisl nunc nisl duis')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('31'), 'in consequat ut nulla sed accumsan felis ut at')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('32'), 'libero rutrum ac lobortis vel dapibus at diam nam')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('33'), 'nec dui luctus rutrum nulla')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('35'), 'ipsum aliquam non mauris morbi non lectus')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('36'), 'imperdiet nullam orci pede venenatis non')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('37'), 'mattis egestas metus aenean fermentum donec ut')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('38'), 'lacus morbi sem mauris laoreet ut rhoncus')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('40'), 'habitasse platea dictumst maecenas ut massa quis')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('41'), 'ut erat id mauris vulputate')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('42'), 'dapibus duis at velit eu est')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('43'), 'hac habitasse platea dictumst morbi')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('44'), 'consequat morbi a ipsum integer a nibh in')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('45'), 'posuere felis sed lacus morbi sem')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('47'), 'non quam nec dui luctus')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('49'), 'erat volutpat in congue etiam justo etiam pretium')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('51'), 'vel lectus in quam fringilla')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('52'), 'pede venenatis non sodales sed tincidunt eu')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('53'), 'a libero nam dui proin leo odio porttitor id')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('54'), 'in purus eu magna vulputate luctus')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('55'), 'elit proin risus praesent lectus vestibulum quam')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('56'), 'in hac habitasse platea dictumst aliquam')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('57'), 'rhoncus dui vel sem sed sagittis nam congue')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('58'), 'nisi volutpat eleifend donec ut dolor morbi')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('59'), 'nisl venenatis lacinia aenean sit')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('61'), 'arcu adipiscing molestie hendrerit at')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('63'), 'cursus urna ut tellus nulla ut erat')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('65'), 'vestibulum quam sapien varius ut blandit')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('66'), 'orci luctus et ultrices posuere cubilia')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('67'), 'accumsan tortor quis turpis sed ante vivamus')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('68'), 'cubilia curae duis faucibus accumsan odio')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('69'), 'dolor vel est donec odio')
INSERT INTO lucas.reports (id, description) VALUES (Decimal('70'), 'quam pede lobortis ligula sit amet eleifend pede')
ALTER TABLE public.reports ADD PRIMARY KEY (id)
CREATE TABLE "lucas.pk_teste" (
"index" INTEGER,
  "id_1" TEXT,
  "id_2" TEXT,
  "description" TEXT
)
INSERT INTO lucas.pk_teste (id_1, id_2, description) VALUES (Decimal('1.0000000000'), Decimal('1'), 'teste')
ALTER TABLE public.pk_teste ADD PRIMARY KEY (id_1,id_2)
CREATE TABLE "lucas.employees" (
"index" INTEGER,
  "id" TEXT,
  "nome" TEXT,
  "departamento" TEXT,
  "superior" TEXT
)
INSERT INTO lucas.employees (id, nome, departamento, superior) VALUES (Decimal('22'), 'Mario', 'IT', Decimal('1'))
INSERT INTO lucas.employees (id, nome, departamento, superior) VALUES (Decimal('1'), 'lucas', 'banco', None)
ALTER TABLE public.employees ADD PRIMARY KEY (id)
CREATE SEQUENCE public.reports_id_seq START WITH 71
ALTER TABLE public.reports ALTER COLUMN "id" SET DEFAULT nextval('public.reports_id_seq')
CREATE SEQUENCE public.pk_teste_id_1_seq START WITH 2
ALTER TABLE public.pk_teste ALTER COLUMN "id_1" SET DEFAULT nextval('public.pk_teste_id_1_seq')
CREATE SEQUENCE public.employees_id_seq START WITH 23
ALTER TABLE public.employees ALTER COLUMN "id" SET DEFAULT nextval('public.employees_id_seq')
ALTER TABLE public.employees ADD CONSTRAINT FK_SUPERIOR FOREIGN KEY (superior) REFERENCES public.employees (id) ON DELETE NO ACTION
CREATE TABLE "lucas.foreign_test" (
"index" INTEGER,
  "id" TEXT,
  "fk_1" TEXT,
  "fk_2" TEXT,
  "fk_3" TEXT
)
INSERT INTO lucas.foreign_test (id, fk_1, fk_2, fk_3) VALUES (Decimal('1.0000000000'), Decimal('1'), Decimal('1'), None)
ALTER TABLE public.foreign_test ADD PRIMARY KEY (id)
CREATE TABLE "lucas.teste" (
"index" INTEGER,
  "id" TEXT,
  "description" TEXT
)
INSERT INTO lucas.teste (id, description) VALUES (Decimal('41'), 'lmao')
INSERT INTO lucas.teste (id, description) VALUES (Decimal('24'), 'teste')
INSERT INTO lucas.teste (id, description) VALUES (Decimal('25'), 'teste_jdbc')
INSERT INTO lucas.teste (id, description) VALUES (Decimal('1'), 'teste1')
INSERT INTO lucas.teste (id, description) VALUES (Decimal('2'), 'teste2')
INSERT INTO lucas.teste (id, description) VALUES (Decimal('3'), 'teste3')
ALTER TABLE public.teste ADD PRIMARY KEY (id)
CREATE SEQUENCE public.foreign_test_id_seq START WITH 2
ALTER TABLE public.foreign_test ALTER COLUMN "id" SET DEFAULT nextval('public.foreign_test_id_seq')
ALTER TABLE public.foreign_test ADD CONSTRAINT FK_TESTE FOREIGN KEY (fk_1,fk_2) REFERENCES public.pk_teste (id_1,id_2) ON DELETE NO ACTION
ALTER TABLE public.foreign_test ADD CONSTRAINT FK_TESTE2 FOREIGN KEY (fk_3) REFERENCES public.employees (id) ON DELETE NO ACTION
CREATE TABLE "lucas.cars" (
"index" INTEGER,
  "id" TEXT,
  "owner" TEXT
)
INSERT INTO lucas.cars (id, owner) VALUES (Decimal('1.0000000000'), Decimal('1'))
INSERT INTO lucas.cars (id, owner) VALUES (Decimal('21.0000000000'), Decimal('1'))
ALTER TABLE public.cars ADD PRIMARY KEY (id)
CREATE SEQUENCE public.cars_id_seq START WITH 22
ALTER TABLE public.cars ALTER COLUMN "id" SET DEFAULT nextval('public.cars_id_seq')
ALTER TABLE public.cars ADD CONSTRAINT FK_OWNER FOREIGN KEY (owner) REFERENCES public.employees (id) ON DELETE NO ACTION
DROP VIEW test_view CASCADE
CREATE OR REPLACE VIEW public.test_view AS select ID, NOME, DEPARTAMENTO, SUPERIOR from employees 
DROP SEQUENCE public.seq_teste CASCADECREATE SEQUENCE public.seq_teste INCREMENT BY 1 START WITH 44CREATE OR REPLACE FUNCTION fn_teste_trig() RETURNS TRIGGER LANGUAGE PLPGSQL AS 
$$
DECLARE
test1 varchar(10) := 'teste1';
test2 varchar(10) := 'teste2';
BEGIN
RAISE NOTICE 'teste1 value: %teste2 value: %', test1 , test2;
RETURN NEW;
END;
$$;

DROP TRIGGER teste_trig CASCADE
CREATE OR REPLACE TRIGGER teste_trig BEFORE INSERT 
ON teste FOR EACH ROW EXECUTE FUNCTION fn_teste_trig();
DROP PROCEDURE car_owner CASCADE
CREATE OR REPLACE PROCEDURE public.car_owner (in owner_id numeric) 
LANGUAGE PLPGSQL AS
$$
DECLARE
owner_name employees.nome%TYPE;
BEGIN
select nome into owner_name from employees where id = owner_id;
RAISE NOTICE 'car owner = %', owner_name ;
EXCEPTION WHEN OTHERS THEN
raise notice 'Transaction has failed and rolledback!';
raise notice '% %', SQLERRM, SQLSTATE;
END;
$$;

CREATE OR REPLACE FUNCTION fn_gera_teste_id() RETURNS TRIGGER LANGUAGE PLPGSQL AS 
$$
begin
select nextval('seq_teste') into NEW.id ;RETURN NEW;

END;
$$;
DROP TRIGGER gera_teste_id CASCADE
CREATE OR REPLACE TRIGGER gera_teste_id before insert 
on teste for each row EXECUTE FUNCTION fn_gera_teste_id();CREATE OR REPLACE FUNCTION fn_car_owner() RETURNS TRIGGER LANGUAGE PLPGSQL AS 
$$
DECLARE
owner_id numeric := TG_ARGV[0];
owner_name employees.nome%TYPE;
begin
select nome into owner_name from employees where id = owner_id;
RAISE NOTICE 'car owner = %', owner_name ;
RETURN NEW;

END;
$$;
DROP TRIGGER trig_test CASCADE
CREATE OR REPLACE TRIGGER trig_test BEFORE INSERT 
ON employees FOR EACH ROW EXECUTE FUNCTION fn_car_owner(1);