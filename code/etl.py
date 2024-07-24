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

def make_txt_file(name, body):
    FILE_PATH = os.path.join(APP_HOME, f'manual_migrations/{name}.txt')
    with open(FILE_PATH, "w") as file:
        file.write(body)
        file.close()

class ETL_session(tk.Toplevel):

    # Dicionário para organizar migração paralela
    dependency_futures = {}
    table_queue = Queue()
    source_queue = Queue()
    S = threading.Semaphore()
    _state = 'idle'

    def __init__(self, master, pg_conf, ora_conf, user, tables, sources, etl, pg_jar, ora_jar, schema):
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
            self.schema = schema
            self.master = master
            print(self.tables)
            print(self.sources)
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
            if self.schema not in pg_schemas:
                cur = self.pg_conn.cursor()
                cur.execute(f"CREATE SCHEMA {self.schema}")
                cur.close()
                self.write2display(f'Schema {self.schema} criado no Postgres!')
            query = f'''SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}' '''
            self.pg_tables = self.execute_spark_query('pg', query)
            query = f'''SELECT proname FROM pg_proc p join pg_namespace n on n.oid = p.pronamespace where nspname = '{self.schema}' '''
            self.pg_source = self.execute_spark_query('pg', query)
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
    def extract_table(self, table_name):
        self.write2display(f'Coletando {table_name}...')
        table_data = {}
        # Pega informações sobre as colunas da tabela
        data = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, dbtable=f'{self.user}.{table_name}').load()
        # Muda os nomes das tabelas para letras minusculas
        data = data.select([col(x).alias(x.lower()) for x in data.columns])
        table_data['data'] = data
        table_data['fk'] = {}
        table_data['auto'] = None
        # Encontra as chaves primarias
        pk_query = f"SELECT cols.table_name, cons.constraint_name, cols.column_name \
                    FROM all_cons_columns cols, all_constraints cons \
                    WHERE cols.owner = '{self.user}' AND cons.owner = '{self.user}' AND cols.table_name = '{table_name}' AND cons.table_name = '{table_name}' \
                    AND cons.constraint_type = 'P' \
                    AND cons.constraint_name = cols.constraint_name \
                    AND cols.owner = cons.owner"
        pk = self.execute_spark_query('ora', pk_query)
        if pk:
            pk = [pk[i]['COLUMN_NAME'] for i in range(len(pk))]
            table_data['pk'] = pk
        # Encontra as chaves estrangeiras
        fk_query = f"SELECT cols.table_name, cons.constraint_name, cols.column_name \
                    FROM all_constraints cons, all_cons_columns cols \
                    WHERE cols.owner = '{self.user}' AND cons.owner = '{self.user}' \
                    AND cols.table_name = '{table_name}' AND cons.table_name = '{table_name}' \
                    AND cons.constraint_type = 'R' \
                    AND cons.constraint_name = cols.constraint_name \
                    AND cols.owner = cons.owner \
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
                            FROM all_cons_columns cols, all_constraints cons \
                            WHERE cols.constraint_name in \
                            (SELECT r_constraint_name FROM all_constraints \
                            WHERE owner = '{self.user}' AND constraint_name = '{constraint}') \
                            AND cols.constraint_name = cons.r_constraint_name \
                            AND cons.owner = '{self.user}' AND cons.table_name = '{table_name}'"
                ref = self.execute_spark_query('ora', ref_query)
                ref_columns = [ref[i]['COLUMN_NAME'] for i in range(len(ref))]
                table_data['fk'][constraint] = {'src_table': table_name,'src_column': columns,'ref_table': ref[0]['TABLE_NAME'], 'ref_column': ref_columns, 'on_delete': ref[0]['DELETE_RULE']}
        # Coleta as dependencias da tabela
        dp_query = f"select name, type, referenced_name, referenced_type from all_dependencies where name in (select name from all_dependencies where referenced_name = '{table_name}')"
        dependencies = self.execute_spark_query('ora', dp_query)
        # Coleta as informações especificas às colunas
        tab_query = f"select column_name, data_type, data_default from all_tab_columns where table_name = '{table_name}'"
        cdata = self.execute_spark_query('ora', tab_query)
        # Identifica e classifica as dependencias
        for dep in dependencies:
            # Chaves primarias com auto incremento definida com trigger
            if dep['TYPE'] == 'TRIGGER' and dep['REFERENCED_TYPE'] == 'SEQUENCE':
                if len(pk) == 1:
                    table_data['auto'] = pk[0]
                else:
                    query = f"select column_name from all_triggers where trigger_name = '{dep['NAME']}'"
                    column = self.execute_spark_query('ora', query)
                    if column[0]:
                        table_data['auto'] = column[0]
                table_data['seq'] = dep['REFERENCED_NAME']
        for column in cdata:
            # Chave primaria com auto incremento definida com IDENTITY
            if column['DATA_DEFAULT']:
                table_data['auto'] = pk[pk.index(column['COLUMN_NAME'])]
                table_data['seq'] = column['DATA_DEFAULT'].split('.')[1]
        if table_name in self.pg_tables:
            self.write2display(f'{table_name} já existe no postgres, substituindo...')
        if fk:
            for value in table_data['fk'].values():
                # Espera pela extração de uma tabela a qual depende
                if (self.dependency_futures[value['ref_table']]._state == 'PENDING' or not self.dependency_futures[value['ref_table']].done()) and value['ref_table'] != table_name:
                    self.write2display(f"Tabela {table_name} depende da tabela {value['ref_table']}, adiando extração...")
                    return ['waiting', table_name, value['ref_table']]
        self.write2display(f"Extração concluída na tabela {table_name}")
        res = self.load_table(table_data, table_name.lower())
        return [res, table_name, None]

    # Carrega dados de uma tabela para a base de dados alvo
    def load_table(self, df, tbl):
        # Estabelece conexão genérica
        cur = self.pg_conn.cursor()
        if tbl in [self.pg_tables[i]['table_name'] for i in range(len(self.pg_tables))]:
            self.write2display(f"Tabela {tbl} já existe no schema {self.schema}, substituindo...")
            cur.execute(f"DROP TABLE {self.schema}.{tbl} CASCADE")
        self.write2display(f"Carregando {df['data'].count()} colunas da tabela {tbl}...")
        try:
            # Carrega a informação extraida sem dependencias ou constraints
            df['data'].repartition(5).write.mode('overwrite').format('jdbc').options(url=self.pg_url, user=self.pg_user, password=self.pg_password, driver=self.pg_driver, dbtable=f'{self.schema}.{tbl}', batch=1000000).save()
            self.write2display(f'Carregando dependencias da tabela {tbl}...')
            # Adiciona dependencia de chave primária
            pk_columns = self.list2str(df['pk'])
            cur.execute(f"ALTER TABLE {self.schema}.{tbl} ADD PRIMARY KEY {pk_columns}")
            if df['auto']:
                last_val = df['data'].agg({df['auto']: 'max'}).collect()[0]
                cur.execute(f'''CREATE SEQUENCE IF NOT EXISTS {tbl}_{df['auto'].lower()}_seq START WITH {int(last_val[f"max({df['auto']})"])}''')
                cur.execute(f'''ALTER TABLE {self.schema}.{tbl} ALTER COLUMN "{df['auto'].lower()}" SET DEFAULT nextval('{tbl}_{df['auto'].lower()}_seq')''')
                cur.execute(f"ALTER SEQUENCE {tbl}_{df['auto'].lower()}_seq OWNER TO postgres")
        except Exception as e:
            self.write2display(f"Carregamento da tabela {tbl} falhou!\nObjetos dependentes não serão migrados!")
            print(e)
            return 'failed'
        # Adiciona dependencias de chaves estrangeiras
        if df['fk']:
            for key, value in df['fk'].items():
                cur.execute(f"ALTER TABLE {self.schema}.{value['src_table'].lower()} ADD CONSTRAINT {key} FOREIGN KEY {self.list2str(value['src_column'])} REFERENCES {value['ref_table'].lower()} {self.list2str(value['ref_column'])} ON DELETE {value['on_delete']}")
        self.write2display(f"{df['data'].count()} colunas importadas da tabela {tbl} para postgres!")
        cur.close()
        return self.test_table(df, tbl)

    # Testa se todas as funcionalidades da tabela foram devidamente migradas
    def test_table(self, df, tbl):
        self.write2display(f"Realizando testes na tabela {tbl}...")
        cur = self.pg_conn.cursor()
        try:
            column_count = self.execute_spark_query('pg', f'SELECT count(*) FROM {self.schema}.{tbl}')
            if column_count[0]['count'] != df['data'].count():
                print(column_count[0]['count'], df['data'].count())
                self.write2display(f'Detectada inconsistencia na tabela {tbl}, tente migra-la novamente!')
                return 'failed'
            cur.execute(f"SELECT con.conname \
                        FROM pg_catalog.pg_constraint con \
                        INNER JOIN pg_catalog.pg_class rel \
                        ON rel.oid = con.conrelid \
                        INNER JOIN pg_catalog.pg_namespace nsp \
                        ON nsp.oid = connamespace \
                        WHERE nsp.nspname = '{self.schema}' \
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
        name = data[0]
        type = data[1]
        self.write2display(f'Extraindo {type} {name}')
        if type == 'VIEW':
            query = f"SELECT text FROM all_views WHERE owner = '{self.user}' AND view_name = '{name}'"
        else:
            query = f"SELECT text, line FROM all_source WHERE owner = '{self.user}' AND name = '{name}' ORDER BY line"
        source = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        query = f"SELECT referenced_name, referenced_type FROM all_dependencies WHERE owner = '{self.user}' AND name = '{name}' AND (referenced_type = 'PROCEDURE' OR referenced_type = 'FUNCTION' OR referenced_type = 'VIEW')"
        dependencies = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
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

    # Remove aspas e adiciona schema ao nome de um objeto
    def normalize_name(self, name):
        if '.' in name:
            name = name.split('.')
        if type(name) == list:
            for i in range(len(name)):
                if '"' in name[i]:
                    name[i] = name[i].removeprefix('"')
                    name[i] = name[i].removesuffix('"')
            if self.schema != 'public':
                return f"{self.schema}.{name[1]}"
            return name[1]
        elif '"' in name:
            name = name.removeprefix('"')
            name = name.removesuffix('"')
        if self.schema != 'public':
            return f"{self.schema}.{name}"
        return name.lower()

    # Traduz elementos oracle genêricos em elementos postgresql
    def transform_attributes(self, tokens):
        began = False
        for i in range(len(tokens)):
            # Procura referencias a tabelas declaradas e adiciona o nome do schema
            if '%TYPE' in tokens[i] and self.schema.lower() != 'public':
                tokens[i] = self.schema + '.' + tokens[i]
            if tokens[i].lower() == 'begin':
                began = True
                continue
            # Procura referencias a tabelas em blocos de sql detectados e adiciona o nome do schema
            if began and ('select' in tokens[i].lower() or 'insert' in tokens[i].lower()):
                aux = i
                while ')' not in tokens[aux].lower() and aux < len(tokens) - 1:
                    if tokens[aux].lower() in ['from', 'into', 'join']:
                        aux += 1
                        query = f"SELECT owner FROM all_views WHERE view_name = '{tokens[i].upper()}'"
                        is_view = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                        if len(is_view) > 0: 
                            if is_view[0]['OWNER'] == 'SYS':
                                # Referencia a meta view, não traduzível!
                                return False    
                        if self.schema.lower() != 'public':
                            tokens[aux] = f'{self.schema}.{tokens[aux]}'
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

    # Postgresql só executa trigger com chamada de função, cria um trigger com a função apropriada
    def create_trigger_function(self, data, name, attributes = None):
        fn_name = f"fn_{name}"
        func_body = f"CREATE OR REPLACE FUNCTION {fn_name}() RETURNS TRIGGER LANGUAGE PLPGSQL AS \n$$\n"
        for i, token in enumerate(data):
            if i == 0:
                # Funções de trigger no postgres não podem receber parâmetros diretamente
                # Força inicialização de parâmetros na área de declarações
                if 'declare' in token.lower():
                    func_body += 'DECLARE\n'
                    if attributes:
                        k = 0
                        for name, type in attributes.items():
                            func_body += f"{name} {type} := TG_ARGV[{k}];\n"
                            k += 1
                    continue
            if token.lower() == 'from':
                if data[i+1].lower() == 'dual;':
                    func_body += ';'
                    continue
            if token.lower() == 'dual;':
                continue
            if token.lower() in ['exception', 'end', 'end;']:
                func_body += 'RETURN NEW;\n'
                break
            if token[-1] == ';' or token.lower() == 'begin':
                func_body += f'{token}\n'
            else:
                func_body += token + ' '
        func_body += '\nEND;\n$$;'
        cur = self.pg_conn.cursor()
        cur.execute(func_body)
        cur.close()
        self.write2display(f"Bloco plsql encapsulado na função {fn_name} e carregado no Postgresql!")
        return fn_name

    # Cria uma função de trigger baseada em uma função/procedure genérica
    def adapt2trig(self, source_name):
        self.write2display(f'Adaptando função {source_name} em trigger...')
        query = f"SELECT text FROM all_source WHERE owner = '{self.user}' AND name = '{source_name.upper()}' ORDER BY line"
        data = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
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
        source_body = f"CREATE OR REPLACE {type} {source_name} "
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
        translatable = self.transform_attributes(tokens)
        if not translatable:
            self.write2display(f'{type} {source_name} não é traduzível e não será migrado!')
            return 'failed'
        self.transform_system_function(tokens)
        looper = self.search4loop(tokens)
        began = False
        factory_func = False
        cursor = None
        unsupported = None
        data_aux = {}
        if type == 'TRIGGER':
            for i in range(2, len(tokens)):
                if 'DBMS_' in tokens[i]:
                    unsupported = tokens[i]
                if tokens[i].lower() == 'on':
                    self.normalize_name(tokens[i+1])
                    data_aux['trig_ref'] = tokens[i+1]
                    source_body += '\n'
                if tokens[i].lower() == 'declare' or tokens[i].lower() == 'begin':
                    # Pl/sql block trigger
                    block_data = tokens[i:]
                    # Adapta bloco pl/sql em trigger_function compatível com Postgres
                    func = self.create_trigger_function(block_data, source_name)
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
                        self.write2display(f'Função {name} não é traduzivel, logo ')
                        return 'failed'
                    source_body += f"EXECUTE FUNCTION {func}"
                    continue
                if i == len(tokens) - 1:
                    source_body += tokens[i] + ';'
                else:
                    source_body += tokens[i] + ' '
        elif type in ['FUNCTION', 'PROCEDURE']:
            i = 2
            while i < len(tokens):
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
            source_body += "EXCEPTION WHEN OTHERS THEN\nraise notice 'Transaction has failed and rolledback!';\nraise notice '% %', SQLERRM, SQLSTATE;"
            source_body += "\nEND;\n$$;"
        elif type == 'VIEW':
            for token in tokens[1:]:
                if '"' in token:
                    if ',' in token:
                        if token[-1] == ',':
                            token = token[:-1]
                        else:
                            aux_token = token.split(',')
                            for i, sub_token in enumerate(aux_token):
                                if '"' in sub_token:
                                    sub_token = sub_token.removeprefix('"')
                                    sub_token = sub_token.removesuffix('"')
                                if i < len(aux_token) - 1:
                                    sub_token += ','
                                source_body += sub_token + ' '
                            continue
                    token = token.removeprefix('"')
                    token = token.removesuffix('"')
                source_body += token + ' '
        cur = self.pg_conn.cursor()
        if factory_func:
            self.write2display(f"Função {source_name} detectada como factory, não é possível garantir o funcionamento da migração\nFunção {source_name} sera adicionada na pasta manual_migrations para migração manual!")
            make_txt_file(source_name, source_body)
            return 'failed'
        if unsupported:
            self.write2display(f"System call não suportada detectada em {source_name}\n{source_name} será adicionado em manual_migrations para migração manual!")
            make_txt_file(source_name, source_body)
            return 'failed'
        if source_name in [self.pg_source[i]['proname'] for i in range(len(self.pg_source))]:
            self.write2display(f'Substituindo {type} {source_name} no Postgresql...')
            cur.execute(f"DROP {type} {source_name} CASCADE;")
        else:
            self.write2display(f'Carregando {type} {source_name} no Postgresql...')
        try:
            print(source_body)
            cur.execute(source_body)
            cur.close()
            self.write2display(f"{type} {source_name} carregado no Postgresql!")
            return self.test_source(data_aux, source_name, type)
        except Exception as e:
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
                            WHERE ns.nspname = '{self.schema}' AND pc.proname = 'fn_{name}'")
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
                            ON cls.relnamespace = ns.oid WHERE ns.nspname = '{self.schema}' AND trig.tgname = '{name}'")
                res = cur.fetchall()
                res = [res[i][0] for i in range(len(res))]
                if ref not in res:
                    self.write2display(f'{type} {name} não foi apropriadamente migrado, tente novamente!')
                    cur.close()
                    return 'failed'
            if type in ['FUNCTION', 'PROCEDURE']:
                cur.execute(f"SELECT * FROM pg_proc AS pc JOIN pg_namespace AS np \
                            ON pc.pronamespace = np.oid \
                            WHERE np.nspname = '{self.schema}' AND pc.proname = '{name}'")
                res = cur.fetchall()
                if len(res) == 0:
                    self.write2display(f'{type} {name} não foi apropriadamente migrado, tente novamente!')
                    cur.close()
                    return 'failed'
                # Talvez criar uma ferramenta que tenta executar a função (difícil)
            if type == 'VIEW':
                cur.execute(f"SELECT viewname FROM pg_views WHERE schemaname = '{self.schema}' AND viewname = '{name}'")
                res = cur.fetchall()
                if len(res) == 0:
                    self.write2display(f'{type} {name} não foi apropriadamente migrado, tente novamente!')
                    cur.close()
                    return 'failed'
                cur.execute(f"SELECT * FROM {name} limit 1")
                res = cur.fetchall()
            cur.close()
            return 'done'
        except:
            self.write2display(f'Falha na tradução do plsql para o {type} {name}! :( )')
            cur.close()
            return 'failed'