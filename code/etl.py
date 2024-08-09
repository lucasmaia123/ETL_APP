import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from multiprocessing import Queue
from pyspark.sql.functions import *
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
import jaydebeapi
import os
import sys
import pathlib
from time import sleep

if getattr(sys, 'frozen', False):
    APP_HOME = os.path.dirname(sys.executable)
else:
    APP_HOME = os.path.dirname(os.path.abspath(__file__))
    APP_HOME = pathlib.Path(APP_HOME).parent

def threaded(func):
    def wrapper(*args, **kwargs):
        return threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()
    return wrapper

def make_txt_file(name, body, error=None):
    FILE_PATH = os.path.join(APP_HOME, f'manual_migrations/{name}.txt')
    with open(FILE_PATH, "w") as file:
        file.write(body)
        file.write('Problema: ' + str(error))
        file.close()

class ETL_session_UI(tk.Toplevel):

    # Dicionário para organizar migração paralela
    dependency_futures = {}
    table_queue = Queue()
    source_queue = Queue()
    S = threading.Semaphore()
    _state = 'idle'

    def __init__(self, master, pg_conf, ora_conf, user, tables, sources, etl, pg_jar, ora_jar, schema, postprocess):
        try:
            super().__init__(master)
            self.protocol('WM_DELETE_WINDOW', self.stop_etl)
            # Strings de conexão de acordo com o padrão jdbc
            # pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}?user={pg_user}&password={pg_password}"
            self.pg_url = f"jdbc:postgresql://{pg_conf['host']}:{pg_conf['port']}/{pg_conf['database']}"
            # ora_url = f"jdbc:oracle:thin:{ora_user}/{ora_password}@//{ora_host}:{ora_port}/{ora_database}"
            self.ora_url = f"jdbc:oracle:thin:@//{ora_conf['host']}:{ora_conf['port']}/{ora_conf['service']}"
            self.pg_driver = "org.postgresql.Driver"
            self.ora_driver = "oracle.jdbc.driver.OracleDriver"
            self.ora_user = ora_conf['user']
            self.ora_password = ora_conf['password']
            self.pg_user = pg_conf['user']
            self.pg_password = pg_conf['password']
            self.user = user
            self.tables = tables
            self.sources = sources
            self.etl = etl
            self.pg_schema = schema
            self.master = master
            self.postprocess = postprocess
            # Conexões genéricas para execução de DML/DDL (PySpark apenas executa queries para coleta de dados)
            # (auto-commit ON, usar a função commit() ou fetch() em operações DML/DDL causará erro,
            #  operações DML/DDL executadas seram aplicadas na base de dados automaticamente!)
            self.pg_conn = jaydebeapi.connect(self.pg_driver, self.pg_url, [pg_conf['user'], pg_conf['password']], pg_jar)
            self.oracle_conn = jaydebeapi.connect(self.ora_driver, self.ora_url, [ora_conf['user'], ora_conf['password']], ora_jar)
        except Exception as e:
            print("Erro de conexão: " + str(e))

    def draw_app_window(self):
        self.geometry('500x600')
        self.information_label = ttk.Label(self, text='Migração em progresso...')
        self.information_label.pack(pady=10)

        self.information_display = ScrolledText(self, wrap=tk.WORD, state='disabled')
        self.information_display.config(height=20, width=50)
        self.information_display.pack(padx=10, pady=10)

        self.button = ttk.Button(self, text='Cancelar', command=self.stop_etl)
        self.button.pack(pady=10, side='bottom')

        self.done_drawing = True

    def execute_spark_query(self, type, query):
        try:
            if type == 'pg':
                res = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
            elif type == 'ora':
                res = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
            return res
        except Exception as e:
            print('Error' + str(e))

    @threaded
    def write2display(self, msg):
        self.S.acquire()
        if msg and self.winfo_exists():
            self.information_display.config(state='normal')
            self.information_display.insert(tk.END, msg + '\n')
            self.information_display.see('end')
            self.information_display.config(state='disabled')
        self.S.release()

    def stop_etl(self):
        for process in self.dependency_futures.values():
            if not process.done():
                self._state = 'failed'
            process.cancel()
            del process
        self.S.acquire()
        if not self.pg_conn._closed:
            self.pg_conn.close()
        if not self.oracle_conn._closed:
            self.oracle_conn.close()
        self.destroy()
        self.S.release()

class Oracle2PostgresETL(ETL_session_UI):
    def __init__(self, master, pg_conf, ora_conf, user, tables, sources, etl, pg_jar, ora_jar, schema):
        super().__init__(master, pg_conf, ora_conf, user, tables, sources, etl, pg_jar, ora_jar, schema)

    def post_processing(self):
        cur = self.pg_conn.cursor()
        for db in self.postprocess['delete_database']:
            try:
                cur.execute(f'DROP DATABASE {db} WITH (FORCE)')
            except:
                window = tk.Toplevel(self.master)
                ttk.Label(window, text=f'Database {db} tem sessões abertas, feche todas as sessões e tente novamente!').pack(padx=20, pady=20)
                ttk.Button(window, text='Ok', command=window.destroy).pack(padx=20, pady=20)
                return 'failed'
        if self.postprocess['create_database']:
            cur.execute(f'CREATE DATABASE {self.postprocess['create_database']}')
        if self.postprocess['owner_database']:
            owner = self.postprocess['owner_database']
            cur.execute(f'GRANT CONNECT ON DATABASE {owner[0]} TO {owner[1]}')
            cur.execute(f'ALTER DATABASE {owner[0]} OWNER TO {owner[1]}')
        if self.postprocess['delete_user']:
            user = self.postprocess['delete_user']
            cur.execute(f'DROP OWNED BY {user} CASCADE')
            cur.execute(f'DROP USER {user}')
        if self.postprocess['create_superuser']:
            user = self.postprocess['create_superuser'][0]
            password = self.postprocess['create_superuser'][1]
            cur.execute(f'CREATE SUPERUSER {user} IDENTIFIED BY {password}')
        elif self.postprocess['create_user']:
            user = self.postprocess['create_user'][0]
            password = self.postprocess['create_user'][1]
            cur.execute(f'CREATE USER {user} IDENTIFIED BY {password}')
        if self.postprocess['connect']:
            self.pg_url =  self.postprocess['connect']['url']
            self.pg_user =  self.postprocess['connect']['user']
            self.pg_password =  self.postprocess['connect']['password']
            self.pg_database =  self.postprocess['connect']['database']
            cur.close()
            self.pg_conn = jaydebeapi.connect(self.pg_driver, self.pg_url, [self.pg_user, self.pg_password], self.pg_jar)
            cur = self.pg_conn.cursor()
        if self.postprocess['create_schema']:
            schema = self.postprocess['create_schema']
            cur.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
            cur.execute(f'CREATE SCHEMA {schema}')
            self.write2log(f'Criado schema {schema} no database {self.pg_database}')
        if self.postprocess['owner_schema']:
            owner = self.postprocess['owner_schema']
            cur.execute(f'GRANT USAGE ON SCHEMA {owner[0]} TO {owner[1]}')
            cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA {owner[0]} GRANT ALL PRIVILEGES ON TABLES TO {owner[1]}')
            cur.execute(f'ALTER SCHEMA {owner[0]} OWNER TO {owner[1]}')
        cur.close()

    # Começa a extrair os dados das tabelas do cluster cujo foi estabelecida a conexão
    @threaded
    def start_etl(self):
        try:
            self._state = 'executing'
            self.done_drawing = False
            self.draw_app_window()
            while not self.done_drawing:
                pass
            query = "SELECT nspname FROM pg_catalog.pg_namespace"
            pg_schemas = self.execute_spark_query('pg', query)
            pg_schemas = [pg_schemas[i]['nspname'] for i in range(len(pg_schemas))]
            if self.pg_schema not in pg_schemas:
                cur = self.pg_conn.cursor()
                cur.execute(f"CREATE SCHEMA {self.pg_schema}")
                cur.close()
                self.write2display(f'Schema {self.pg_schema} criado no Postgres!')
            query = f'''SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.pg_schema}' '''
            self.pg_tables = self.execute_spark_query('pg', query)
            self.pg_tables = [self.pg_tables[i]['table_name'] for i in range(len(self.pg_tables))]
            query = f'''SELECT proname FROM pg_proc p join pg_namespace n on n.oid = p.pronamespace where nspname = '{self.pg_schema}' '''
            self.pg_source = self.execute_spark_query('pg', query)
            self.pg_source = [self.pg_source[i]['proname'] for i in range(len(self.pg_source))]
            # Executa extração de cada tabela/source em threads assincronas
            # O número no método define quantos objetos serão extraídos simultaneamente
            # Aumentar este valor fará o processo mais rápido mas poderá causar instabilidades
            with ThreadPoolExecutor(5) as executor:
                failures = []
                for table in self.tables:
                    self.table_queue.put_nowait(table)
                sleep(1)
                while not self.table_queue.empty():
                    while not self.table_queue.empty():
                        table = self.table_queue.get_nowait()
                        print(f'table {table}')
                        self.dependency_futures[table[1]] = executor.submit(self.extract_table, table)
                    for future in as_completed(self.dependency_futures.values()):
                        res = future.result()
                        name = res[1][1]
                        dep = res[2]
                        if res[0] == 'failed':
                            failures.append(name)
                        if res[0] == 'waiting':
                            self.dependency_futures[name].cancel()
                            if dep in failures:
                                failures.append(name)
                            if name not in failures:
                                self.table_queue.put_nowait(res[1])
                wait(list(self.dependency_futures.values()))
                self.dependency_futures = {}
                for source in self.sources:
                    self.source_queue.put_nowait(source)
                sleep(1)
                while not self.source_queue.empty():
                    while not self.source_queue.empty():
                        source = self.source_queue.get_nowait()
                        print(f'source {source}')
                        self.dependency_futures[source[1]] = executor.submit(self.extract_source, source)
                    for future in as_completed(self.dependency_futures.values()):
                        res = future.result()
                        name = res[1][1]
                        dep = res[2]
                        if res[0] == 'failed':
                            failures.append(name)
                        if res[0] == 'waiting':
                            self.dependency_futures[name].cancel()
                            if dep in failures:
                                failures.append(name)
                            if name not in failures:
                                self.source_queue.put_nowait(res[1])
                executor.shutdown(wait=True)
            sleep(1)
            self.write2display('Concluído!')
            if len(failures) > 0:
                self.write2display(f'Falhas: {failures}')
            self.information_label.config(text='Migração concluída!')
            self.button.config(text='Ok')
            self._state = 'success'
        except Exception as e:
            self.write2display('Migração falhou!')
            self._state = 'failed'
            self.error_message = e

    # Coleta informações sobre uma tabela do cluster
    def extract_table(self, table):
        table_schema = table[0]
        table_name = table[1]
        self.write2display(f'Coletando {table_schema}.{table_name}...')
        table_data = {}
        # Pega informações sobre as colunas da tabela
        data = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, dbtable=f'{table_schema.lower()}.{table_name.lower()}').load()
        # Muda os nomes das tabelas para letras minusculas
        data = data.select([col(x).alias(x.lower()) for x in data.columns])
        table_data['data'] = data
        table_data['fk'] = {}
        table_data['auto'] = None
        # Encontra as chaves primarias
        pk_query = f"SELECT cols.table_name, cons.constraint_name, cols.column_name \
                    FROM all_cons_columns cols JOIN all_constraints cons ON cols.owner = cons.owner and cols.table_name = cons.table_name \
                    WHERE cols.owner = '{table_schema}' AND cols.table_name = '{table_name}' \
                    AND cons.constraint_type = 'P' \
                    AND cons.constraint_name = cols.constraint_name \
                    AND cols.owner = cons.owner"
        pk = self.execute_spark_query('ora', pk_query)
        if pk:
            pk = [pk[i]['COLUMN_NAME'] for i in range(len(pk))]
            table_data['pk'] = pk
        # Encontra as chaves estrangeiras
        fk_query = f"SELECT cols.table_name, cons.constraint_name, cols.column_name \
                    FROM all_constraints cons JOIN all_cons_columns cols ON cons.owner = cols.owner \
                    AND cons.table_name = cols.table_name AND cons.owner = cols.owner \
                    AND cons.constraint_name = cols.constraint_name \
                    WHERE cols.owner = '{table_schema}' AND cols.table_name = '{table_name}' \
                    AND cons.constraint_type = 'R' \
                    ORDER BY cols.table_name"
        fk = self.execute_spark_query('ora', fk_query)
        fk_dict = {}
        # Organiza as chaves estrangeiras em relação aos nomes de seus constraints
        for key in fk:
            if key['CONSTRAINT_NAME'] in fk_dict.keys():
                fk_dict[key['CONSTRAINT_NAME']].append(key['COLUMN_NAME'])
            else:
                fk_dict[key['CONSTRAINT_NAME']] = [key['COLUMN_NAME']]
        if fk:
            # Encontra as referencias das chaves estrangeiras
            for constraint, columns in fk_dict.items():
                ref_query = f"SELECT cols.table_name, cols.column_name, cons.delete_rule, cols.owner \
                            FROM all_cons_columns cols JOIN all_constraints cons ON cols.constraint_name = cons.r_constraint_name \
                            WHERE cons.constraint_name = '{constraint}' AND cons.owner = '{table_schema}' AND cons.table_name = '{table_name}'"
                ref = self.execute_spark_query('ora', ref_query)
                ref_columns = [ref[i]['COLUMN_NAME'] for i in range(len(ref))]
                table_data['fk'][constraint] = {'src_owner': table_schema, 'src_table': table_name, 'src_column': columns, 'ref_owner': ref[0]['OWNER'], 'ref_table': ref[0]['TABLE_NAME'], 'ref_column': ref_columns, 'on_delete': ref[0]['DELETE_RULE']}
        # Coleta as informações especificas às colunas
        tab_query = f"select column_name, data_type, data_default from all_tab_columns where table_name = '{table_name}' AND owner = '{table_schema}'"
        cdata = self.execute_spark_query('ora', tab_query)
        # Checa se chave primaria com auto incremento foi definida com IDENTITY
        for column in cdata:
            if column['DATA_DEFAULT']:
                table_data['auto'] = pk[pk.index(column['COLUMN_NAME'])]
                table_data['seq'] = column['DATA_DEFAULT'].split('.')[1]
        if fk:
            for value in table_data['fk'].values():
                # Espera pela extração de uma tabela a qual depende
                if (self.dependency_futures[value['ref_table']]._state == 'PENDING' or not self.dependency_futures[value['ref_table']].done()) and value['ref_table'] != table_name:
                    self.write2display(f"Tabela {table_name} depende da tabela {value['ref_table']}, adiando extração...")
                    return ['waiting', table, value['ref_table']]
        self.write2display(f"Extração concluída na tabela {table_schema}.{table_name}")
        res = self.load_table(table_data, table)
        return [res, table, None]

    # Carrega dados de uma tabela para a base de dados alvo
    def load_table(self, df, tbl):
        table_name = tbl[1].lower()
        # Estabelece conexão genérica
        cur = self.pg_conn.cursor()
        if table_name in self.pg_tables:
            self.write2display(f"Tabela {table_name} já existe no schema {self.pg_schema}, substituindo...")
            cur.execute(f"DROP TABLE {self.pg_schema}.{table_name} CASCADE")
        self.write2display(f"Carregando {df['data'].count()} colunas da tabela {table_name}...")
        try:
            # Carrega a informação extraida sem dependencias ou constraints
            df['data'].repartition(5).write.mode('overwrite').format('jdbc').options(url=self.pg_url, user=self.pg_user, password=self.pg_password, driver=self.pg_driver, dbtable=f'{self.pg_schema}.{table_name}', batch=1000000).save()
            self.write2display(f'Carregando dependencias da tabela {table_name}...')
            # Adiciona dependencia de chave primária
            pk_columns = self.list2str(df['pk'])
            cur.execute(f"ALTER TABLE {self.pg_schema}.{table_name} ADD PRIMARY KEY {pk_columns}")
            if df['auto']:
                last_val = df['data'].agg({df['auto']: 'max'}).collect()[0]
                cur.execute(f"SELECT cls.relname FROM pg_sequence seq JOIN pg_class cls \
                            ON seq.seqrelid = cls.oid JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace \
                            WHERE cls.relname = '{table_name}_{df['auto'].lower()}_seq' AND nsp.nspname = '{self.pg_schema}'")
                res = cur.fetchall()
                if len(res) == 0:
                    cur.execute(f'''CREATE SEQUENCE {self.pg_schema}.{table_name}_{df['auto'].lower()}_seq START WITH {int(last_val[f"max({df['auto']})"]) + 1}''')
                else:
                    cur.execute(f'''ALTER SEQUENCE {self.pg_schema}.{table_name}_{df['auto'].lower()}_seq RESTART WITH {int(last_val[f"max({df['auto']})"]) + 1}''')
                cur.execute(f'''ALTER TABLE {self.pg_schema}.{table_name} ALTER COLUMN "{df['auto'].lower()}" SET DEFAULT nextval('{self.pg_schema}.{table_name}_{df['auto'].lower()}_seq')''')
                cur.execute(f"ALTER SEQUENCE {self.pg_schema}.{table_name}_{df['auto'].lower()}_seq OWNER TO {self.pg_user}")
        except Exception as e:
            self.write2display(f"Carregamento da tabela {table_name} falhou!\nObjetos dependentes não serão migrados!")
            print(e)
            return 'failed'
        # Adiciona dependencias de chaves estrangeiras
        if df['fk']:
            for key, value in df['fk'].items():
                cur.execute(f"ALTER TABLE {self.pg_schema}.{value['src_table'].lower()} ADD CONSTRAINT {key} FOREIGN KEY {self.list2str(value['src_column'])} REFERENCES {self.pg_schema}.{value['ref_table'].lower()} {self.list2str(value['ref_column'])} ON DELETE {value['on_delete']}")
        self.write2display(f"{df['data'].count()} colunas importadas da tabela {table_name} para postgres!")
        cur.close()
        return self.test_table(df, table_name)

    # Testa se todas as funcionalidades da tabela foram devidamente migradas
    def test_table(self, df, tbl):
        self.write2display(f"Realizando testes na tabela {tbl}...")
        cur = self.pg_conn.cursor()
        try:
            column_count = self.execute_spark_query('pg', f'SELECT count(*) FROM {self.pg_schema}.{tbl}')
            if column_count[0]['count'] != df['data'].count():
                self.write2display(f'Detectada inconsistencia no número de colunas da tabela {tbl}, tente migra-la novamente!')
                return 'failed'
            cur.execute(f"SELECT con.conname \
                        FROM pg_catalog.pg_constraint con \
                        INNER JOIN pg_catalog.pg_class rel \
                        ON rel.oid = con.conrelid \
                        INNER JOIN pg_catalog.pg_namespace nsp \
                        ON nsp.oid = connamespace \
                        WHERE nsp.nspname = '{self.pg_schema}' \
                        AND rel.relname = '{tbl}'")
            cons = cur.fetchall()
            cons = [cons[i][0] for i in range(len(cons))]
            cur.execute(f"SELECT ccu.column_name FROM information_schema.constraint_column_usage AS ccu \
                        JOIN information_schema.table_constraints AS tc ON ccu.constraint_name = tc.constraint_name \
                        WHERE tc.table_name = '{tbl}' AND tc.constraint_type = 'PRIMARY KEY'")
            pk_columns = cur.fetchall()
            pk_columns = [pk_columns[i][0] for i in range(len(pk_columns))]
            for column in pk_columns:
                if column.upper() not in df['pk']:
                    self.write2display(f'Detectada inconsistencia na tabela {tbl}, tente migra-la novamente!')
                    return 'failed'
            if df['fk']:
                for key in df['fk'].keys():
                    if key.lower() not in cons:
                        self.write2display(f'Detectada inconsistencia na tabela {tbl}, tente migra-la novamente!')
                        return 'failed'
            cur.close()
            self.write2display(f'Tabela {tbl} foi migrada com sucesso!')
            return 'done'
        except Exception as e:
            self.write2display(f'Algo deu errado, tabela {tbl} não foi migrada!')
            print('Error ' + str(e))
            return 'failed'

    def list2str(self, lista):
        res = '('
        for value in lista:
            res += str(value.lower())
            if value != lista[-1]:
                res += ','
        res += ')'
        return res
    
    # Organizador para extração paralela de sources
    def extract_source(self, data):
        owner = data[0]
        name = data[1]
        type = data[2]
        self.write2display(f'Extraindo {type} {name}')
        if type == 'SEQUENCE':
            # Sequencias não precisam de tradução, então já a adiciona imediatamente
            cur = self.oracle_conn.cursor()
            cur.execute(f"select last_number, increment_by from all_sequences where sequence_owner = '{owner}' and sequence_name = '{name}'")
            res = cur.fetchall()
            last_val = res[0][0] + 1
            increment = res[0][1]
            cur.close()
            cur = self.pg_conn.cursor()
            cur.execute(f"SELECT cls.relname FROM pg_sequence seq JOIN pg_class cls \
                ON seq.seqrelid = cls.oid JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace \
                WHERE cls.relname = '{name.lower()}' AND nsp.nspname = '{self.pg_schema}'")
            res = cur.fetchall()
            if len(res):
                cur.execute(f"DROP SEQUENCE {self.pg_schema}.{name.lower()} CASCADE")
            cur.execute(f"CREATE SEQUENCE {self.pg_schema}.{name.lower()} INCREMENT BY {increment} START WITH {last_val}")
            self.write2display(f'Sequence {self.pg_schema}.{name.lower()} inserido no Postgres!')
            return ['done', data, None]
        if type == 'VIEW':
            query = f"SELECT text FROM all_views WHERE owner = '{owner}' AND view_name = '{name}'"
        else:
            query = f"SELECT text, line FROM all_source WHERE owner = '{owner}' AND name = '{name}' ORDER BY line"
        source = self.execute_spark_query('ora', query)
        query = f"SELECT referenced_name, referenced_type FROM all_dependencies WHERE owner = '{owner}' AND name = '{name}' AND referenced_type in ('PROCEDURE', 'FUNCTION', 'VIEW', 'SEQUENCE')"
        dependencies = self.execute_spark_query('ora', query)
        if type == 'VIEW':
            source_body = [f"{self.normalize_name(name)} AS {source[0]['TEXT']}"]
        else:
            source_body = [source[i]['TEXT'] for i in range(len(source))]
        dependency = [[dependencies[i]['REFERENCED_NAME'], dependencies[i]['REFERENCED_TYPE']] for i in range(len(dependencies))]
        for dep in dependency:
            if (self.dependency_futures[dep[0]]._state == 'PENDING' or not self.dependency_futures[dep[0]].done()) and dep[0] != name:
                self.write2display(f'Source {name} depende do source {dep[0]}, adiando extração...')
                return ['waiting', data, dep[0]]
        res = self.transform_source(source_body, self.normalize_name(name), type)
        return [res, data, None]

    # Remove aspas e schema do nome de um objeto
    def normalize_name(self, name):
        if '.' in name:
            name = name.split('.')
        if type(name) == list:
            for i in range(len(name)):
                if '"' in name[i]:
                    name[i] = name[i].removeprefix('"')
                    name[i] = name[i].removesuffix('"')
            if name[0].lower() not in [self.pg_schema, 'public']:
                return None
            return name[1].lower()
        elif '"' in name:
            name = name.removeprefix('"')
            name = name.removesuffix('"')
        return name.lower()

    # Traduz elementos oracle genêricos em elementos postgresql
    def transform_attributes(self, tokens):
        began = False
        for i in range(len(tokens)):
            # Procura referencias a tabelas declaradas e adiciona o nome do schema
            if '%TYPE' in tokens[i] and self.pg_schema.lower() != 'public':
                tokens[i] = self.pg_schema + '.' + tokens[i]
            if tokens[i].lower() == 'begin':
                began = True
                continue
            # Procura referencias a tabelas em blocos de sql detectados e adiciona o nome do schema
            if began and ('select' in tokens[i].lower() or 'insert' in tokens[i].lower()):
                aux = i
                while ')' not in tokens[aux].lower() and aux < len(tokens) - 1:
                    if tokens[aux].lower() in ['from', 'join']:
                        aux += 1
                        query = f"SELECT owner FROM all_views WHERE view_name = '{tokens[i].upper()}'"
                        is_view = self.execute_spark_query('ora', query)
                        if len(is_view) > 0: 
                            if is_view[0]['OWNER'] == 'SYS':
                                # Referencia a meta view, não traduzível!
                                return False    
                        if self.pg_schema.lower() != 'public' and 'dual' not in tokens[aux]:
                            tokens[aux] = f'{self.pg_schema}.{tokens[aux]}'
                    aux += 1
            if tokens[i].lower() in ['in', 'out', 'in out'] and not began:
                aux = tokens[i]
                tokens.pop(i)
                if '(' in tokens[i-1]:
                    tokens[i-1] = tokens[i-1][1:]
                    aux = '(' + str(aux)
                else:
                    aux = ',' + str(aux)
                tokens.insert(i-1, aux)
            if tokens[i].lower() == 'cursor':
                tokens[i], tokens[i+1] = tokens[i+1], tokens[i]
                if tokens[i+2].lower() == 'is':
                    tokens[i+2] = 'FOR'
            if '.' in tokens[i]:
                tokens[i] = tokens[i].replace(':new', 'NEW')
                tokens[i] = tokens[i].replace(':old', 'OLD')
                if 'nextval' in tokens[i]:
                    aux = tokens[i].split('.')
                    tokens[i] = f"nextval('{aux[0]}')"
            if 'number' in tokens[i].lower():
                tokens[i] = tokens[i].lower().replace('number', 'numeric')
            if 'varchar2' in tokens[i].lower():
                tokens[i] = tokens[i].lower().replace('varchar2', 'varchar')
            if tokens[i].lower() != 'exception':
                # Remove variáveis de exceção
                if 'exception' in tokens[i].lower():
                    tokens.remove(tokens[i])
                    tokens.remove(tokens[i-1])
                # Postgresql não possui pragma
                if tokens[i].lower() == 'pragma':
                    tokens.remove(tokens[i+1])
                    tokens.remove(tokens[i])
                # Modifica funções de raise
                if tokens[i].lower() == 'raise':
                    exc_name = tokens[i+1]
                    tokens.replace(tokens[i+1], 'EXCEPTION')
                    tokens.insert(i+2, f"'EXCEPTION_{exc_name}'")
                if 'raise' in tokens[i].lower() and '(' in tokens[i]:
                    exc_aux = ''
                    while ')' not in tokens[i]:
                        exc_aux += tokens[i] + ' '
                        tokens.remove(tokens[i])
                    exc_aux += tokens[i]
                    tokens.remove(tokens[i])
                    aux_line = f"RAISE EXCEPTION '{exc_aux}'".split()
                    aux_line.reverse()
                    for token in aux_line:
                        tokens.insert(i, token)
        return True

    # Muda o formato de algumas system calls do Oracle para Postgresql
    def transform_system_function(self, tokens):
        for i in range(len(tokens)):
            # Muda dbms_output para raise notice
            if 'dbms_output' in tokens[i].lower():
                end = start = i
                while end < len(tokens):
                    if '(' in tokens[end]:
                        if tokens[end].count(')') > 1:          
                            break
                    elif ')' in tokens[end] or ';' in tokens[end]:
                        break
                    end += 1
                # Delimita o escopo da função e a separa do corpo
                data = tokens[start:end+1]
                data = ' '.join(data)
                text_data = data.split('\'')
                attribute_data = data.split('||')
                for text in text_data:
                    if 'dbms_output' in text or '||' in text:
                        text_data.remove(text)
                for attribute in attribute_data:
                    if '\'' in attribute:
                        attribute_data.remove(attribute)
                new_line = "RAISE NOTICE "
                new_line += '\''
                for text in text_data:
                    new_line += text + ' %'
                new_line += '\''
                for attribute in attribute_data:
                    if ')' in attribute:
                        attribute = attribute[:attribute.index(')')] + ';'
                    new_line += ',' + attribute
                # Remove os tokens modificados e insere os novos
                new_line = new_line.split()
                new_line.reverse()
                for j in range(start, end+1):
                    tokens.pop(start)
                for token in new_line:
                    tokens.insert(start, token)
                self.transform_system_function(tokens)
                break
            # Muda dbms_lock para pg_sleep
            if 'dbms_lock' in tokens[i].lower():
                start = tokens[i].index('(')
                end = tokens[i].index(')')
                delay = tokens[i][start+1:end]
                tokens.remove(tokens[i])
                tokens.insert(i, f"pg_sleep({delay});")
                tokens.insert(i, 'PERFORM')
                self.transform_system_function(tokens)
                break

    # Postgresql só executa trigger com chamada de função, isso cria um trigger com a função apropriada
    def create_trigger_function(self, data, name, attributes = None):
        if self.pg_schema != 'public':
            fn_name = f"{self.pg_schema}.fn_{name}"
        else:
            fn_name = f"fn_{name}"
        func_body = f"CREATE OR REPLACE FUNCTION {fn_name}() RETURNS TRIGGER LANGUAGE PLPGSQL AS \n$$\n"
        for i in range(len(data)):
            if i == 0:
                # Funções de trigger no postgres não podem receber parâmetros diretamente
                # Força inicialização de parâmetros na área de declarações
                if 'declare' in data[i].lower():
                    func_body += 'DECLARE\n'
                    if attributes:
                        k = 0
                        for name, type in attributes.items():
                            func_body += f"{name} {type} := TG_ARGV[{k}];\n"
                            k += 1
                    continue
            if 'DBMS_' in data[i]:
                return data[i]
            if data[i].lower() == 'from':
                if data[i+1].lower() == 'dual;':
                    func_body += ';'
                    continue
            if data[i].lower() == 'dual;':
                continue
            if data[i].lower() == 'on':
                if self.pg_schema != 'public':
                    data[i+1] = f"{self.pg_schema}.{data[i+1]}"
            if data[i].lower() in ['exception', 'end', 'end;']:
                func_body += 'RETURN NEW;\n'
                break
            if data[i][-1] == ';' or data[i].lower() == 'begin':
                func_body += f'{data[i]}\n'
            else:
                func_body += data[i] + ' '
        func_body += '\nEND;\n$$;'
        cur = self.pg_conn.cursor()
        cur.execute(func_body)
        cur.close()
        self.write2display(f"Bloco plsql encapsulado na função {fn_name}!")
        return fn_name

    # Cria uma função de trigger baseada em uma função/procedure genérica
    def adapt2trig(self, source_name):
        query = f"SELECT text FROM all_source WHERE owner = '{self.user}' AND name = '{source_name.upper()}' ORDER BY line"
        data = self.execute_spark_query('ora', query)
        data_body = [data[i]['TEXT'] for i in range(len(data))]
        tokens = []
        # Quebra o corpo dos dados em tokens para processamento
        for i, line in enumerate(data_body):
            line = line.split()
            # Checa de o nome está concatenado com os atributos
            if i == 0:
                if '(' in line[1]:
                    name = line[1][:line[1].index('(')]
                    source_name = self.normalize_name(name)
                    line[1] = line[1][line[1].index('(')-1:]
                    line.insert(1, source_name)
                else:
                    source_name = self.normalize_name(line[1])
                    if not source_name:
                        return 'failed'
                    line[1] = source_name
            for token in line:
                tokens.append(token)
        translatable = self.transform_attributes(tokens)
        if not translatable:
            self.write2display(f'Função {source_name} não é traduzível e não pode ser migrada!')
            return 'failed'
        self.transform_system_function(tokens)
        attributes = {}
        i = 2
        while ')' not in tokens[i-1]:
            if '(' in tokens[i]:
                tokens[i] = tokens[i][1:]
            if tokens[i].lower() in ['in', 'out', 'in out']:
                if ')' in tokens[i+2]:
                    attributes[tokens[i+1]] = tokens[i+2][:-1]
                else:
                    attributes[tokens[i+1]] = tokens[i+2]
                i += 3
            else:
                if ')' in tokens[i+1]:
                    attributes[tokens[i]] = tokens[i+1][:-1]
                else:
                    attributes[tokens[i]] = tokens[i+1]
                i += 2
        while tokens[i].lower() not in ['as', 'is']:
            i += 1
        if tokens[i+1].lower() != 'begin':
            tokens.insert(i+1, 'DECLARE\n')
        return self.create_trigger_function(tokens[i+1:], source_name, attributes)

    # Pequena ferramenta para encontrar loops em blocos pl/sql
    def search4loop(self, tokens):
        began = False
        looper = None
        for i in range(len(tokens)):
            if tokens[i].lower() == 'begin':
                began = True
            if not began:
                continue
            else:
                if tokens[i].lower() == 'for':
                    looper = tokens[i+1]
                    break
        return looper

    # Faz transformações no escopo do tipo de precedure
    def transform_source(self, data, source_name, type):
        if not source_name:
            return 'failed'
        if type == 'TRIGGER':
            # Em Postgres, triggers herdam o schema da tabela relacionada
            source_body = f"CREATE OR REPLACE {type} {source_name} "
        else:
            source_body = f"CREATE OR REPLACE {type} {self.pg_schema}.{source_name} "
        tokens = []
        # Quebra o corpo dos dados em tokens para processamento
        for i, line in enumerate(data):
            line = line.split()
            # Checa de o nome está concatenado com os atributos
            if i == 0 and type != 'VIEW' and '(' in line[1]:
                name = line[1][:line[1].index('(')]
                line[1] = line[1][line[1].index('('):]
                line.insert(1, name)
            for token in line:
                tokens.append(token)
        # Transforma atributos genêricos a todos os tipos
        translatable = self.transform_attributes(tokens)
        if not translatable:
            self.write2display(f'{type} {source_name} não é traduzível e não será migrado!')
            return 'failed'
        # Transforma algumas funções do sistema Oracle para versão apropriada em Postgres
        self.transform_system_function(tokens)
        # Procura loops no corpo da source
        looper = self.search4loop(tokens)
        began = False
        factory_func = False
        cursor = None
        unsupported = None
        data_aux = {}
        if type == 'TRIGGER':
            for i in range(2, len(tokens)):
                # Procura função do sistema que não foi transformada
                if 'DBMS_' in tokens[i]:
                    unsupported = tokens[i]
                if tokens[i].lower() == 'on':
                    if not self.normalize_name(tokens[i+1]):
                        return 'failed'
                    data_aux['trig_ref'] = tokens[i+1]
                    if self.pg_schema != 'public':
                        tokens[i+1] = f'{self.pg_schema}.{tokens[i+1]}'
                    source_body += '\n'
                if tokens[i].lower() == 'declare' or tokens[i].lower() == 'begin':
                    # Pl/sql block trigger
                    block_data = tokens[i:]
                    # Adapta bloco pl/sql em trigger_function compatível com Postgres
                    func = self.create_trigger_function(block_data, source_name)
                    if 'DBMS_' in data[i]:
                        unsupported = func
                    data_aux['trig_func'] = func
                    source_body += f"EXECUTE FUNCTION {func}();"
                    break
                # Function call trigger
                if tokens[i].lower() == 'call':
                    # Separa nome e atributos
                    if '(' in tokens[i+1]:
                        name = tokens[i+1][:tokens[i+1].index('(')]
                        tokens[i+1] = tokens[i+1][tokens[i+1].index('('):]
                    else:
                        name = tokens[i+1]
                        tokens.remove(tokens[i+1])
                    # Coleta corpo da função e adapta como trigger_function compatível com Postgres
                    func = self.adapt2trig(name)
                    if func == 'failed':
                        return 'failed'
                    data_aux['trig_func'] = func
                    source_body += f"EXECUTE FUNCTION {func}"
                    continue
                if i == len(tokens) - 1:
                    source_body += tokens[i] + ';'
                else:
                    source_body += tokens[i] + ' '
        elif type in ['FUNCTION', 'PROCEDURE']:
            i = 2
            while i < len(tokens):
                # Caso encontre chamada a DBMS, significa que uma system call não foi traduzida
                if 'DBMS_' in tokens[i]:
                    unsupported = tokens[i]
                if tokens[i].lower() == 'return':
                    tokens[i] = 'RETURNS'
                if tokens[i].lower() == 'begin':
                    if began:
                        # Lascou
                        factory_func = True
                    else:
                        began = True
                        source_body += 'BEGIN\n'
                        i += 1
                        continue
                if tokens[i].lower() == 'cursor':
                    cursor = tokens[i-1]
                if tokens[i].lower() in ['is', 'as']:
                    source_body += '\nLANGUAGE PLPGSQL AS\n$$\nDECLARE\n'
                    # Checa de o looper referencia o cursor, caso não, define o loop como record
                    if looper and looper != cursor:
                        source_body += f'{looper} record;\n'
                    i += 1
                    continue
                if tokens[i].lower() == 'for' and began:
                    if '(' in tokens[i+3]:
                        tokens[i+3] = tokens[i+3][1:]
                        j = i+3
                        while ')' not in tokens[j]:
                            j += 1
                        tokens[j] = tokens[j][:-1]
                if tokens[i].lower() == 'execute':
                    if tokens[i+1].lower() == 'immediate':
                        source_body += 'EXECUTE'
                        i += 2
                        continue
                if tokens[i].lower() in ['exception', 'end', 'end;']:
                    if tokens[i].lower() == 'end':
                        if tokens[i+1].lower() == 'loop;':
                            source_body += 'end loop;'
                            i += 2
                            continue
                    source_body += '\n'
                    break
                source_body += tokens[i]
                if tokens[i][-1] == ';' or tokens[i].lower() == 'loop':
                    source_body += '\n'
                else:
                    source_body += ' '
                i += 1
            # Adiciona exceção genêrica
            source_body += "EXCEPTION WHEN OTHERS THEN\nraise notice 'Transaction has failed and rolledback!';\nraise notice '% %', SQLERRM, SQLSTATE;"
            source_body += "\nEND;\n$$;"
        elif type == 'VIEW':
            for i in range(1, len(tokens)):
                # Remove aspas dos parametros da view, separa parametros concatenados e adiciona schema
                if '"' in tokens[i]:
                    if ',' in tokens[i]:
                        if tokens[i][-1] == ',':
                            tokens[i] = tokens[i][:-1]
                        else:
                            aux_token = tokens[i].split(',')
                            for i, sub_token in enumerate(aux_token):
                                if '"' in sub_token:
                                    sub_token = sub_token.removeprefix('"')
                                    sub_token = sub_token.removesuffix('"')
                                if i < len(aux_token) - 1:
                                    sub_token += ','
                                source_body += sub_token + ' '
                            continue
                    tokens[i] = tokens[i].removeprefix('"')
                    tokens[i] = tokens[i].removesuffix('"')
                if tokens[i].lower() == 'from':
                    if self.pg_schema != 'public':
                        tokens[i+1] = f'{self.pg_schema}.{tokens[i+1]}'
                source_body += tokens[i] + ' '
        cur = self.pg_conn.cursor()
        if factory_func:
            self.write2display(f"Função {source_name} detectada como factory, não é possível garantir o funcionamento da migração\nFunção {source_name} sera adicionada na pasta manual_migrations para migração manual!")
            make_txt_file(source_name, source_body, 'Função tipo factory')
            return 'failed'
        if unsupported:
            self.write2display(f"Função de sistema não suportada detectada em {source_name}\n{source_name} será adicionado em manual_migrations para migração manual!")
            make_txt_file(source_name, source_body, f'Função {unsupported} não suportada pelo aplicativo')
            return 'failed'
        if source_name in self.pg_source:
            self.write2display(f'Substituindo {type} {source_name} no Postgresql...')
            cur.execute(f"DROP {type} {source_name} CASCADE;")
        else:
            self.write2display(f'Carregando {type} {source_name} no Postgresql...')
        try:
            cur.execute(source_body)
            cur.close()
            self.write2display(f"{type} {source_name} carregado no Postgresql!")
            return self.test_source(data_aux, source_name, type)
        except Exception as e:
            self.write2display(f'Algo deu errado na tradução do {type} {source_name}! Adicionado para migração manual!')
            make_txt_file(source_name, source_body, e)
            print(f'Error: ' + str(e))

    # Testa se a tradução do plsql ocorreu sem erros
    def test_source(self, data, name, type):
        self.write2display(f'Iniciando testes no {type} {name}...')
        cur = self.pg_conn.cursor()
        try:
            if type == 'TRIGGER':
                # Checa se a função trigger existe
                cur.execute(f"SELECT tg.tgname FROM pg_proc AS pc \
                            JOIN pg_trigger AS tg ON pc.oid = tg.tgfoid \
                            JOIN pg_namespace AS ns ON pc.pronamespace = ns.oid \
                            WHERE ns.nspname = '{self.pg_schema}' AND pc.proname = '{self.normalize_name(data['trig_func'])}'")
                res = cur.fetchall()
                res = [res[i][0] for i in range(len(res))]
                if name not in res:
                    self.write2display(f'{type} {name} não foi apropriadamente migrado, tente novamente!')
                    cur.close()
                    return 'failed'
                ref = data['trig_ref']
                # Checa se o trigger está associado a tabela
                cur.execute(f"SELECT cls.relname FROM pg_trigger trig \
                            JOIN pg_class AS cls ON cls.oid = trig.tgrelid JOIN pg_namespace AS ns \
                            ON cls.relnamespace = ns.oid WHERE ns.nspname = '{self.pg_schema}' AND trig.tgname = '{name}'")
                res = cur.fetchall()
                res = [res[i][0] for i in range(len(res))]
                if ref not in res:
                    self.write2display(f'{type} {name} não foi apropriadamente migrado, tente novamente!')
                    cur.close()
                    return 'failed'
            if type in ['FUNCTION', 'PROCEDURE']:
                # Checa se função existe
                cur.execute(f"SELECT proname FROM pg_proc AS pc JOIN pg_namespace AS np \
                            ON pc.pronamespace = np.oid \
                            WHERE np.nspname = '{self.pg_schema}' AND pc.proname = '{name}'")
                res = cur.fetchall()
                if len(res) == 0:
                    self.write2display(f'{type} {name} não foi apropriadamente migrado, tente novamente!')
                    cur.close()
                    return 'failed'
                # Talvez criar uma ferramenta que tenta executar a função (difícil)
            if type == 'VIEW':
                # Checa se view existe
                cur.execute(f"SELECT viewname FROM pg_views WHERE schemaname = '{self.pg_schema}' AND viewname = '{name}'")
                res = cur.fetchall()
                if len(res) == 0:
                    self.write2display(f'{type} {name} não foi apropriadamente migrado, tente novamente!')
                    cur.close()
                    return 'failed'
                # Testa query na view
                cur.execute(f"SELECT * FROM {self.pg_schema}.{name} limit 1")
                res = cur.fetchall()
            cur.close()
            self.write2display(f'testes no {type} {name} concluídos sem problemas!')
            return 'done'
        except Exception as e:
            self.write2display(f'Falha na tradução do plsql para o {type} {name}! :( )')
            self.write2log('Error ' + str(e))
            cur.close()
            return 'failed'
        
class Postgres2OracleETL(ETL_session_UI):
    def __init__(self, master, pg_conf, ora_conf, user, tables, sources, etl, pg_jar, ora_jar, schema):
        super().__init__(master, pg_conf, ora_conf, user, tables, sources, etl, pg_jar, ora_jar, schema)

    # Começa a extrair os dados das tabelas do cluster cujo foi estabelecida a conexão
    @threaded
    def start_etl(self):
        try:
            self._state = 'executing'
            self.done_drawing = False
            self.draw_app_window()
            while not self.done_drawing:
                pass
            query = f'''SELECT table_name FROM all_users WHERE owner = '{self.user}' AND tablespace_name not like 'SYS%' '''
            self.ora_tables = self.execute_spark_query('ora', query)
            self.ora_tables = [self.ora_tables[i]['TABLE_NAME'] for i in range(len(self.ora_tables))]
            query = f"SELECT name, type FROM all_source WHERE owner = '{self.pg_schema}' AND line = 1 \
                    UNION SELECT view_name, 'VIEW' FROM all_views WHERE owner = '{self.pg_schema}'"
            self.ora_sources = self.execute_spark_query('ora', query)
            self.ora_sources = [[self.ora_sources[i]['NAME'], self.ora_sources[i]['TYPE']] for i in range(len(self.ora_sources))]
            # Executa extração de cada tabela/source em threads assincronas
            # O número no método define quantos objetos serão extraídos simultaneamente
            # Aumentar este valor fará o processo mais rápido mas poderá causar instabilidades
            with ThreadPoolExecutor(5) as executor:
                failures = []
                for table in self.tables:
                    self.table_queue.put_nowait(table)
                sleep(1)
                while not self.table_queue.empty():
                    while not self.table_queue.empty():
                        table = self.table_queue.get_nowait()
                        print(f'table {table}')
                        self.dependency_futures[table] = executor.submit(self.extract_table, table)
                    for future in as_completed(self.dependency_futures.values()):
                        res = future.result()
                        name = res[1]
                        dep = res[2]
                        if res[0] == 'failed':
                            failures.append(name)
                        if res[0] == 'waiting':
                            self.dependency_futures[name].cancel()
                            if dep in failures:
                                failures.append(name)
                            if name not in failures:
                                self.table_queue.put_nowait(name)
                wait(list(self.dependency_futures.values()))
                self.dependency_futures = {}
                for source in self.sources:
                    self.source_queue.put_nowait(source)
                sleep(1)
                while not self.source_queue.empty():
                    while not self.source_queue.empty():
                        source = self.source_queue.get_nowait()
                        print(f'source {source}')
                        self.dependency_futures[source[0]] = executor.submit(self.extract_source, source)
                    for future in as_completed(self.dependency_futures.values()):
                        res = future.result()
                        name = res[1][0]
                        dep = res[2]
                        if res[0] == 'failed':
                            failures.append(name)
                        if res[0] == 'waiting':
                            self.dependency_futures[name].cancel()
                            if dep in failures:
                                failures.append(name)
                            if name not in failures:
                                self.source_queue.put_nowait(res[1])
                executor.shutdown(wait=True)
            sleep(1)
            self.write2display('Concluído!')
            if len(failures) > 0:
                self.write2display(f'Falhas: {failures}')
            self.information_label.config(text='Migração concluída!')
            self.button.config(text='Ok')
            self._state = 'success'
        except Exception as e:
            self.write2display('Migração falhou!')
            self._state = 'failed'
            self.error_message = e

    # Coleta informações sobre uma tabela do cluster
    def extract_table(self, table):
        table_schema = table[0]
        table_name = table[1]
        self.write2display(f'Coletando {table_schema}.{table_name}...')
        table_data = {}
        # Pega informações sobre as colunas da tabela
        data = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, dbtable=f'{table_schema}.{table_name}').load()
        table_data['data'] = data
        table_data['fk'] = {}
        table_data['auto'] = None
        # Encontra as chaves primarias
        pk_query = f"SELECT ccu.column_name FROM information_schema.table_constraints tc \
                    JOIN information_schema.constraint_column_usage AS ccu ON tc.constraint_name = ccu.constraint_name \
                    WHERE tc.table_schema = '{table_schema}' AND tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'"
        pk = self.execute_spark_query('pg', pk_query)
        if pk:
            pk = [pk[i]['COLUMN_NAME'] for i in range(len(pk))]
            table_data['pk'] = pk
        # Encontra as chaves estrangeiras
        fk_query = f"SELECT tc.constraint_name, kcu.column_name \
                    FROM information_schema.table_constraints tc \
                    JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name \
                    WHERE tc.table_schema = '{table_schema}' AND tc.table_name = '{table_name}' AND tc.constraint_type = 'FOREIGN KEY'"
        fk = self.execute_spark_query('pg', fk_query)
        fk_dict = {}
        # Organiza as chaves estrangeiras em relação aos nomes de seus constraints
        for key in fk:
            if key['constraint_name'] in fk_dict.keys():
                fk_dict[key['constraint_name']].append(key['column_name'])
            else:
                fk_dict[key['constraint_name']] = [key['column_name']]
        if fk:
            # Encontra as referencias das chaves estrangeiras
            for constraint, columns in fk_dict.items():
                ref_query = f"select ccu.table_schema, ccu.table_name, ccu.column_name, rc.delete_rule \
                            from information_schema.constraint_column_usage ccu \
                            JOIN information_schema.referential_constraints AS rc ON ccu.constraint_name = rc.constraint_name \
                            WHERE ccu.constraint_name = '{constraint}'"
                ref = self.execute_spark_query('pg', ref_query)
                ref_columns = [ref[i]['column_name'] for i in range(len(ref))]
                table_data['fk'][constraint] = {'src_schema': table_schema, 'src_table': table_name, 'src_column': columns, 'ref_schema': ref[0]['table_schema'], 'ref_table': ref[0]['table_name'], 'ref_column': ref_columns, 'on_delete': ref[0]['delete_rule']}
        # Coleta as informações especificas às colunas
        tab_query = f"SELECT * FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}'"
        cdata = self.execute_spark_query('pg', tab_query)
        # Identifica e classifica as dependencias
        for column in cdata:
            # Chave primaria com auto incremento definida com IDENTITY
            if column['column_default']:
                table_data['auto'] = pk[pk.index(column['column_name'])]
                table_data['seq'] = column['column_default'][column['column_default'].index('\''):]
                table_data['seq'] = table_data['seq'][:table_data['seq'].index('\'')]
        if table_name in self.ora_tables:
            self.write2display(f'{table_name} já existe no postgres, substituindo...')
        if fk:
            for value in table_data['fk'].values():
                # Espera pela extração de uma tabela a qual depende
                if (self.dependency_futures[value['ref_table']]._state == 'PENDING' or not self.dependency_futures[value['ref_table']].done()) and value['ref_table'] != table_name:
                    self.write2display(f"Tabela {table_name} depende da tabela {value['ref_table']}, adiando extração...")
                    return ['waiting', table_name, value['ref_table']]
        self.write2display(f"Extração concluída na tabela {table_name}")
        res = self.load_table(table_data, table_name)
        return [res, table_name, None]

    # Carrega dados de uma tabela para a base de dados alvo
    def load_table(self, df, tbl):
        # Estabelece conexão genérica
        cur = self.oracle_conn.cursor()
        if tbl in self.ora_tables:
            self.write2display(f"Tabela {tbl} já existe no schema {self.user}, substituindo...")
            cur.execute(f"DROP TABLE {self.user}.{tbl} CASCADE")
        self.write2display(f"Carregando {df['data'].count()} colunas da tabela {tbl}...")
        try:
            # Carrega a informação extraida sem dependencias ou constraints
            df['data'].repartition(5).write.mode('overwrite').format('jdbc').options(url=self.ora_url, user=self.ora_user, password=self.ora_password, driver=self.ora_driver, dbtable=f'{self.user}.{tbl}', batch=1000000).save()
            self.write2display(f'Carregando dependencias da tabela {tbl}...')
            # Adiciona dependencia de chave primária
            pk_columns = self.list2str(df['pk'])
            cur.execute(f"ALTER TABLE {self.user}.{tbl} ADD CONSTRAINT PRIMARY KEY {pk_columns}")
            if df['auto']:
                last_val = df['data'].agg({df['auto']: 'max'}).collect()[0]
                cur.execute(f"SELECT sequence_name FROM all_sequences WHERE sequence_name = '{tbl.upper()}_{df['auto'].upper()}_seq'")
                res = cur.fetchall()
                if len(res):
                    cur.execute(f"DROP SEQUENCE {tbl.upper()}_{df['auto'].upper()}_seq")
                cur.execute(f'''CREATE SEQUENCE {tbl.upper()}_{df['auto'].upper()}_seq START WITH {int(last_val[f"max({df['auto']})"])}''')
                cur.execute(f'''ALTER TABLE {self.pg_schema}.{tbl} MODIFY "{df['auto'].upper()}" DEFAULT {tbl}_{df['auto'].lower()}_seq.nextval ''')
        except Exception as e:
            self.write2display(f"Carregamento da tabela {tbl} falhou!\nObjetos dependentes não serão migrados!")
            print(e)
            return 'failed'
        # Adiciona dependencias de chaves estrangeiras
        if df['fk']:
            for key, value in df['fk'].items():
                cur.execute(f"ALTER TABLE {self.user}.{value['src_table'].upper()} ADD CONSTRAINT {key} FOREIGN KEY {self.list2str(value['src_column'])} REFERENCES {self.user}.{value['ref_table'].lower()} {self.list2str(value['ref_column'])} ON DELETE {value['delete_rule']}")
        self.write2display(f"{df['data'].count()} colunas importadas da tabela {tbl} para postgres!")
        cur.close()
        return self.test_table(df, tbl)
    
    def test_table(self, df, tbl):
        self.write2display(f"Realizando testes na tabela {tbl}...")
        cur = self.pg_conn.cursor()
        try:
            column_count = self.execute_spark_query('ora', f'SELECT count(*) FROM {self.user}.{tbl}')
            if column_count[0]['count'] != df['data'].count():
                self.write2display(f'Detectada inconsistencia no número de colunas da tabela {tbl}, tente migra-la novamente!')
                return 'failed'
            cur.execute(f"SELECT cols.column_name FROM all_constraints cons \
                        JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name \
                        WHERE constraint_type = 'P' AND cons.owner = '{self.user}' AND cons.table_name = '{tbl}'")
            pk_columns = cur.fetchall()
            pk_columns = [pk_columns[i][0] for i in range(len(pk_columns))]
            for column in pk_columns:
                if column.lower() not in df['pk']:
                    self.write2display(f'Detectada inconsistencia na tabela {tbl}, tente migra-la novamente!')
                    return 'failed'
            if df['fk']:
                cur.execute(f"SELECT DISTINCT cons.constraint_name FROM all_constraints cons JOIN all_cons_columns cols \
                            ON cons.constraint_name = cols.constraint_name WHERE cons.constraint_type = 'R' \
                            AND cons.owner = '{self.user}' AND cons.table_name = '{tbl}'")
                cons = cur.fetchall()
                cons = [cons[i][0] for i in range(len(cons))]
                for key in df['fk'].keys():
                    if key.upper() not in cons:
                        self.write2display(f'Detectada inconsistencia na tabela {tbl}, tente migra-la novamente!')
                        return 'failed'
            cur.close()
            self.write2display(f'Tabela {tbl} foi migrada com sucesso!')
            return 'done'
        except Exception as e:
            self.write2display(f'Algo deu errado, tabela {tbl} não foi migrada!')
            print('Error ' + str(e))
            return 'failed'