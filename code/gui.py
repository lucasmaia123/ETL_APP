import threading
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from copy import deepcopy
from time import sleep
import psycopg2
import oracledb
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from ttkwidgets import tooltips
from code.etl import Oracle2PostgresETL, Postgres2OracleETL
from subprocess import PIPE, Popen
import os
import sys
import pathlib
import datetime
import shlex

if getattr(sys, 'frozen', False):
    APP_HOME = os.path.dirname(sys.executable)
else:
    APP_HOME = os.path.dirname(os.path.abspath(__file__))
    APP_HOME = pathlib.Path(APP_HOME).parent
LOG_FILE_PATH = os.path.join(APP_HOME, 'logs.txt')

# Decorator para invocar função como thread
def threaded(func):
    def wrapper(*args, **kwargs):
        return threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()
    return wrapper

ORA_DRIVER = "oracle.jdbc.driver.OracleDriver"
PG_DRIVER = "org.postgresql.Driver"

class etl_UI(tk.Frame):

    no_backup = False
    active_sessions = []

    def __init__(self, master, conf, pg_jar, ora_jar):
        self.master = master
        self.master.title('ETL APP PROTOTYPE')
        super().__init__(master)
        self.pack()
        self.conf = conf
        self.pg_jar = pg_jar
        self.ora_jar = ora_jar
        self.pg_conn = None
        self.ora_conn = None
        self.etl = None
        s = ttk.Style(master)
        s.configure('White.TRadiobutton', background='white', foreground='black')
        s.configure('White.TCheckbutton', background='white', foreground='black')
        self.master.protocol('WM_DELETE_WINDOW', self.end_process)
        self.draw_start_menu()

    # Limpa processos da memória e fecha o app
    def end_process(self):
        if self.pg_conn:
            if not self.pg_conn.closed:
                self.pg_conn.close()
        if self.ora_conn:
            self.ora_conn.close()
        if self.etl:
            self.etl.stop()
        self.master.quit()
        self.master.destroy()

    # Desenha menu de login
    def draw_start_menu(self):

        # limpa a janela
        for child in self.winfo_children():
            child.destroy()
        if self.etl:
            self.etl.stop()
        super().__init__(self.master)
        self.pack()

        upper_frame = ttk.Frame(self)
        upper_frame.pack(side='top', expand=True, fill='x')
        middle_frame = ttk.Frame(self)
        middle_frame.pack(fill='x')
        lower_frame = ttk.Frame(self)
        lower_frame.pack(fill='x', pady=20)

        self.conn_mode = tk.StringVar()

        # Desenha os gadgets do app
        ttk.Label(upper_frame, text='Especifique as configurações para conexão nas bases de dados').pack(pady=20)
        ttk.Label(middle_frame, text='Oracle host').grid(row=0, column=1, padx=5, pady=10)
        self.ora_host_entry = ttk.Entry(middle_frame, width=30)
        self.ora_host_entry.grid(row=1, column=1)
        ttk.Label(middle_frame, text='Oracle port').grid(row=2, column=1, padx=5, pady=10)
        self.ora_port_entry = ttk.Entry(middle_frame, width=30)
        self.ora_port_entry.grid(row=3, column=1)
        ttk.Label(middle_frame, text='Oracle username').grid(row=4, column=1, padx=5, pady=10)
        self.ora_user_entry = ttk.Entry(middle_frame, width=30)
        self.ora_user_entry.grid(row=5, column=1)
        ttk.Label(middle_frame, text='Oracle password').grid(row=6, column=1, padx=5, pady=10)
        self.ora_password_entry = ttk.Entry(middle_frame, width=30, show='*')
        self.ora_password_entry.grid(row=7, column=1)
        ttk.Label(middle_frame, text='Oracle service Name').grid(row=8, column=1, padx=5, pady=10)
        self.ora_service_mode = ttk.Radiobutton(middle_frame, variable=self.conn_mode, value='name')
        self.ora_service_mode.grid(row=9, column=0)        
        self.ora_service_entry = ttk.Entry(middle_frame, width=30)
        self.ora_service_entry.grid(row=9, column=1)
        ttk.Label(middle_frame, text='Oracle SID').grid(row=10, column=1, padx=5, pady=10)
        self.ora_SID_mode = ttk.Radiobutton(middle_frame, variable=self.conn_mode, value='SID')
        self.ora_SID_mode.grid(row=11, column=0, padx=10)
        self.ora_SID_entry = ttk.Entry(middle_frame, width=30)
        self.ora_SID_entry.grid(row=11, column=1, padx=10)

        ttk.Button(middle_frame, text='Criar script Oracle -> Postgres', command=lambda:self.initialize_conn(response_label, 'script2postgres'), tooltip='Use para gerar um script e migrar manualmente\nRequer conexão a um usuário Oracle').grid(row=12, column=1, padx=10, pady=10)

        self.ora_service_mode.config(command=lambda:[self.ora_service_entry.config(state='normal'), self.ora_SID_entry.config(state='disabled')])
        self.ora_SID_mode.config(command=lambda:[self.ora_service_entry.config(state='disabled'), self.ora_SID_entry.config(state='normal')])

        self.ora_service_mode.invoke()

        ttk.Label(middle_frame, text='Postgresql host').grid(row=0, column=2, padx=20, pady=10)
        self.pg_host_entry = ttk.Entry(middle_frame, width=30)
        self.pg_host_entry.grid(row=1, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql port').grid(row=2, column=2, padx=20, pady=10)
        self.pg_port_entry = ttk.Entry(middle_frame, width=30)
        self.pg_port_entry.grid(row=3, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql username').grid(row=4, column=2, padx=20, pady=10)
        self.pg_user_entry = ttk.Entry(middle_frame, width=30)
        self.pg_user_entry.grid(row=5, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql password').grid(row=6, column=2, padx=20, pady=10)
        self.pg_password_entry = ttk.Entry(middle_frame, width=30, show='*')
        self.pg_password_entry.grid(row=7, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql database').grid(row=8, column=2, padx=20, pady=10)
        self.pg_database_entry = ttk.Entry(middle_frame, width=30)
        self.pg_database_entry.grid(row=9, column=2, padx=20)

        ttk.Button(middle_frame, text='Criar script Postgres -> Oracle', tooltip='Use para gerar um script e migrar manualmente\nRequer conexão a um usuário Postgresql').grid(row=12, column=2, padx=10, pady=10)

        self.migration_direction = tk.IntVar()

        Oracle2PostgresButton = ttk.Radiobutton(lower_frame, text='Oracle -> Postgres', state='normal', variable=self.migration_direction, value=1, tooltip='Define direção Oracle para Postgres quando usando migração direta')
        Oracle2PostgresButton.pack(padx=10)

        Postgres2OracleButton = ttk.Radiobutton(lower_frame, text='Postgres -> Oracle', state='normal', variable=self.migration_direction, value=2, tooltip='Define direção Postgres para Oracle quando usando migração direta')
        Postgres2OracleButton.pack(padx=10)

        Oracle2PostgresButton.invoke()

        response_label = ttk.Label(lower_frame, text='')
        response_label.pack(padx=10, pady=10)

        test_button = ttk.Button(lower_frame, text='Testar conexão', command=lambda: self.initialize_conn(response_label, 'test'), tooltip='Testa se é possível alcançar as bases de dados com as credenciais providas')
        test_button.pack(side='left', padx=20, pady=20)
        direct_migration_button = ttk.Button(lower_frame, text='Prosseguir com migração direta', command=lambda: self.initialize_conn(response_label, 'direct'), tooltip='Migração rápida utilizando multiprocessamento\nRequer conexão direta com ambas as bases de dados\nMigração direta não gera scripts')
        direct_migration_button.pack(side='right', padx=20, pady=20)

    # Coleta os atributos que o usuário forneceu e cria variáveis de conexão
    def initialize_conn(self, response,  mode=''):

        self.ora_host = self.ora_host_entry.get()
        self.ora_port = self.ora_port_entry.get()
        self.ora_user = self.ora_user_entry.get()
        self.ora_password = self.ora_password_entry.get()
        if self.conn_mode.get() == 'name':
            self.ora_service = self.ora_service_entry.get()
        if self.conn_mode.get() == 'SID':
            self.ora_service = self.ora_SID_entry.get()

        self.pg_host = self.pg_host_entry.get()
        self.pg_port = self.pg_port_entry.get()
        self.pg_user = self.pg_user_entry.get()
        self.pg_password = self.pg_password_entry.get()
        self.pg_database = self.pg_database_entry.get()

        if '' in [self.ora_host, self.ora_port, self.ora_user, self.ora_password, self.ora_service] and '' in [self.pg_host, self.pg_port, self.pg_user, self.pg_password, self.pg_database]:
            response.config(text='Preencha todos os parâmetros de conexão!')
            return

        conns = self.conn_test()
        if mode == 'test':
            if conns[0] and conns[1]:
                response.config(text='Conexão estabelecida com Oracle e Postgres!')
            elif conns[0]:
                if conns[1] == False:
                    response.config(text='Conexão estabelecida com Postgres\nConexão falhou com Oracle!')
                else:
                    response.config(text='Conexão estabelecida com Postgres!')
            elif conns[1]:
                if conns[0] == False:
                    response.config(text='Conexão estabelecida com Oracle\nConexão falhou com Postgres!')
                else:
                    response.config(text='Conexão estabelecida com Oracle!')
            else:
                response.config(text='Conexão falhou! Cheque se as credenciais estão corretas')

        # Estabelece conexão para criação de script postgres para oracle
        if conns[0] and mode == 'script2oracle':
            pass

        # Estabelece conexão para criação de script oracle para postgres
        elif conns[1] and mode == 'script2postgres':
            try:
                etl = SparkSession(SparkContext(conf=self.conf)).newSession()
            except:
                self.message_window('Sessão de ETL já iniciada!')
                response.config(text='')
                return
            self.ora_dba = False
            if self.ora_user == 'sys':
                self.ora_dba = True
            else:
                try:
                    query = f"SELECT granted_role FROM dba_role_privs WHERE grantee = '{self.ora_user.upper()}'"
                    priv = etl.read.format('jdbc').options(driver=ORA_DRIVER, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                    priv = [priv[i]['GRANTED_ROLE'] for i in range(len(priv))]
                    if 'DBA' in priv:
                        self.ora_dba = True
                except:
                    pass
            Oracle2Postgres(self, etl, 'script')
            response.config(text='')

        # Estabelece conexão para migração direta
        elif all(conns) and mode == 'direct':
            response.config(text='Conectando...')
            self.update()
            # Inicializa uma sessão do pyspark para começar a migração
            try:
                etl = SparkSession(SparkContext(conf=self.conf)).newSession()
            except:
                self.message_window('Sessão de ETL já iniciada!')
                response.config(text='')
                return
            self.ora_dba = False
            if self.ora_user == 'sys':
                self.ora_dba = True
            else:
                try:
                    query = f"SELECT granted_role FROM dba_role_privs WHERE grantee = '{self.ora_user.upper()}'"
                    priv = etl.read.format('jdbc').options(driver=ORA_DRIVER, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                    priv = [priv[i]['GRANTED_ROLE'] for i in range(len(priv))]
                    if 'DBA' in priv:
                        self.ora_dba = True
                except:
                    pass
            # Checa se é superuser no Postgresql
            query = f"SELECT usesuper FROM pg_user WHERE usename = '{self.pg_user}'"
            superuser = etl.read.format('jdbc').options(driver=PG_DRIVER, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
            superuser = superuser[0]['usesuper']
            if superuser:
                self.pg_superuser = True
            else:
                self.pg_superuser = False
            if self.migration_direction.get() == 1:
                Oracle2Postgres(self, etl, 'direct')
                etl.read.format('jdbc').options()   
            if self.migration_direction.get() == 2:
                self.message_window('Não implementado!')
                etl.stop()
            response.config(text='')
        elif mode == 'direct':
            if conns[0] == False and conns[1] == True:
                response.config(text='Migração direta requer conexão a ambas as bases de dados\nConexão a postgres falhou!')
            elif conns[0] == True and conns[1] == False:
                response.config(text='Migração direta requer conexão a ambas as bases de dados\nConexão a oracle falhou!')
            else:
                response.config(text='Migração direta requer conexão a ambas as bases de dados\nConexão a postgres e oracle falharam!')

    # Testa as credenciais de conexão fornecidas
    def conn_test(self):
        conn = []
        if len(self.pg_user) and len(self.pg_password):
            try:
                self.pg_url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"
                if self.pg_conn != None:
                    self.pg_conn.close()
                self.pg_conn = psycopg2.connect(dbname=self.pg_database, user=self.pg_user, password=self.pg_password, host=self.pg_host, port=self.pg_port)
                conn.append(True)
            except:
                conn.append(False)
        else:
            self.pg_url = None
            self.pg_conn = None
            conn.append(None)
        if len(self.ora_user) and len(self.ora_password):
            try:
                if self.conn_mode.get() == 'name':
                    self.ora_url = f"jdbc:oracle:thin:@//{self.ora_host}:{self.ora_port}/{self.ora_service}"
                if self.conn_mode.get() == 'SID':
                    self.ora_url = f"jdbc:oracle:thin:@{self.ora_host}:{self.ora_port}:{self.ora_service}"
                if self.ora_conn != None:
                    self.ora_conn.close()
                if self.ora_user.lower() == 'sys':
                    auth_mode = oracledb.AUTH_MODE_SYSDBA
                else:
                    auth_mode = oracledb.AUTH_MODE_DEFAULT
                if self.conn_mode.get() == 'name':
                    self.ora_conn = oracledb.connect(user=self.ora_user, password=self.ora_password, host=self.ora_host, port=self.ora_port, service_name=self.ora_service, mode=auth_mode)
                else:
                    self.ora_conn = oracledb.connect(user=self.ora_user, password=self.ora_password, host=self.ora_host, port=self.ora_port, sid=self.ora_service, mode=auth_mode)
                conn.append(True)
            except:
                conn.append(False)
        else:
            self.ora_url = None
            self.ora_conn = None
            conn.append(None)
        return conn

    # Ferramenta para mensagem de erros
    def message_window(self, message):
        window = tk.Toplevel(self.master)
        window.title('Mensagem')
        ttk.Label(window, text=message).pack(padx=20, pady=10)
        ttk.Button(window, text='Ok', command=lambda: window.destroy()).pack(side='bottom', padx=10, pady=10)

    # Escreve log no logs.txt
    def write2log(self, data):
        with open(LOG_FILE_PATH, "a") as log_file:
            data = datetime.datetime.strftime(datetime.datetime.now(), "%d/%m/%Y %H:%M:%S") + f' Sessão {self.__hash__()}: ' + data + '\n'
            log_file.write(data)
            log_file.close()

# Classe para preparação do ETL de Oracle para Postgres
class Oracle2Postgres(etl_UI):
    
    def __init__(self, master, conn_session, mode):
        self.title('Oracle para Postgres')
        self.etl = conn_session
        # Black magic
        for key in dir(super()):
            if key[0] != '_' and key:
                exec(f'self.{key} = master.{key}')
        self.master = master
        self.mode = mode
        self.full_backup = False
        self.deleted_objects = []
        # Setup será usado para guardar operações DDL/DML que devem ser executados antes da migração
        self.setup = {}
        self.setup['create_user'] = None
        self.setup['create_superuser'] = None
        self.setup['delete_user'] = None
        self.setup['create_database'] = []
        self.setup['owner_database'] = None
        self.setup['delete_database'] = []
        self.setup['create_schema'] = None
        self.setup['delete_schema'] = []
        self.setup['owner_schema'] = None
        self.setup['connect'] = None
        if self.mode == 'direct':
            self.write2log(f'Iniciando sessão {self.__hash__()} de Oracle {self.ora_url} para Postgres {self.pg_url}')
            self.pg_users = self.execute_query('pg', 'SELECT usename FROM pg_user')
            self.pg_users = [self.pg_users[i][0] for i in range(len(self.pg_users))]
            self.pg_schemas = self.execute_query('pg', 'SELECT nspname FROM pg_namespace')
            self.pg_schemas = [self.pg_schemas[i][0] for i in range(len(self.pg_schemas))]
            self.pg_databases = self.execute_query('pg', 'SELECT datname FROM pg_database')
            self.pg_databases = [self.pg_databases[i][0] for i in range(len(self.pg_databases))]
        if self.ora_dba:
            self.draw_schema_selection_window()
        else:
            self.draw_table_selection_window(self.ora_user.upper())

    def execute_query(self, type, query):
        try:
            if type == 'pg':
                cur = self.pg_conn.cursor()
                cur.execute(query)
                res = cur.fetchall()
                cur.close()
            elif type == 'ora':
                cur = self.ora_conn.cursor()
                cur.execute(query)
                res = cur.fetchall()
                cur.close()
            return res
        except Exception as e:
            print('Error' + str(e))

    # Cria um backup de um schema ou de toda a base de dados antes de realizar o ETL.
    def create_backup(self, schema=None):
        try:
            # Backup utiliza o pg_dump, logo é necessário ter o Postgresql instalado mesmo que esteja fazendo a conexão com um servidor não local
            if schema:
                BACKUP_FILE = os.path.join(APP_HOME, f'backups/{schema}_backup.dmp')
                command = f"pg_dump --no-password -h {self.pg_host} -p {self.pg_port} -d {self.pg_database} -U {self.pg_user} -n {schema} -Fc -f {BACKUP_FILE}"
            else:
                BACKUP_FILE = os.path.join(APP_HOME, f'backups/{self.pg_database}_database_backup.dmp')
                command = f"pg_dump --no-password -h {self.pg_host} -p {self.pg_port} -d {self.pg_database} -U {self.pg_user} -Fc -f {BACKUP_FILE}"
            window = tk.Toplevel(self)
            label = ttk.Label(window, text='Fazendo backup da base de dados')
            label.pack(padx=10, pady=10)
            ttk.Button(window, text='Cancelar', command=self.close).pack(padx=10, pady=10)
            self.backup_window(window, label)
            sleep(0.5)
            p = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, env={'PGPASSWORD': self.pg_password})
            p.wait()
            window.destroy()
            return True
        except:
            self.write2log('pg_dump não instalado, backup não efetuado!')
            if self.window.winfo_exists():
                self.window.destroy()
            window = tk.Toplevel(self)
            var = tk.BooleanVar()
            var.set(False)
            ttk.Label(window, text='pg_dump não encontrado, prosseguir sem fazer backup?\n(Para realizar o backup, por favor instale o SGBD Postgres nesta máquina)').pack(padx=10, pady=10)
            ttk.Button(window, text='Não', command=window.destroy).pack(side='left', padx=20, pady=20)
            ttk.Button(window, text='Sim', command=lambda:[var.set(True), window.destroy()]).pack(side='right', padx=20, pady=20)
            self.wait_window(window)
            return var.get()
    
    @threaded
    # Pequena animação enquanto o backup ocorre
    def backup_window(self, window, label):
        while window.winfo_exists():
            sleep(0.5)
            if label.cget("text") == 'Fazendo backup da base de dados...':
                label['text'] = 'Fazendo backup da base de dados'
            else:
                label['text'] += '.'

    # Restora ao estado antes do ETL
    def restore_from_backup(self, schema=None):
        if schema:
            BACKUP_FILE = os.path.join(APP_HOME, f'backups/{schema}_backup.dmp')
        else:
            BACKUP_FILE = os.path.join(APP_HOME, f'backups/{self.__hash__()}_database_backup.dmp')
        command = f"pg_restore --no-password -h {self.pg_host} -p {self.pg_port} -d {self.pg_database} -U {self.pg_user} {BACKUP_FILE}"
        command = shlex.split(command)
        p = Popen(command, shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, env={'PG_PASSWORD': self.pg_password})
        p.wait()

    # Deleta arquivo de backup
    def remove_backup(self, schema=None):
        if schema:
            BACKUP_FILE = os.path.join(APP_HOME, f'backups/{schema}_backup.dmp')
        else:
            BACKUP_FILE = os.path.join(APP_HOME, f'backups/{self.__hash__()}_database_backup.dmp')
        try:
            os.remove(BACKUP_FILE)
        except:
            self.write2log("Arquivo de backup não encontrado, prosseguindo...")

    # Caso usuário de sistema oracle, lista os schemas da base de dados
    def draw_schema_selection_window(self):
        for frame in self.winfo_children():
           frame.destroy()
        self.geometry('800x600')

        # Coleta usuários (schemas em Oracle) que não são padrões do Oracle
        query = "SELECT username FROM all_users WHERE oracle_maintained = 'N'"
        schemas = self.execute_query('ora', query)
        schemas = [schemas[i][0] for i in range(len(schemas))]

        ttk.Label(self, text='Conectado como usuário do sistema\nSelecione o usuário/schema que deseja migrar').pack(padx=20, pady=10)
        user_frame = ttk.Frame(self, height=300)
        user_frame.pack(fill='x', expand=True)

        scrollBar = tk.Scrollbar(user_frame, orient='vertical')
        scrollBar.pack(side='right', fill='y')

        listbox = tk.Listbox(user_frame, selectmode='single', exportselection=False, yscrollcommand=scrollBar.set)
        listbox.pack(padx=10, pady=10, expand=True, fill='both')

        for schema in schemas:
            listbox.insert(tk.END, schema)

        scrollBar.config(command=listbox.yview)

        self.migration_mode = tk.IntVar()

        lower_frame = ttk.Frame(self)
        lower_frame.pack()
        mmref = ttk.Checkbutton(lower_frame, text='Migrar objetos (Seleciona objetos do schema para serem migrados)', variable=self.migration_mode, onvalue=0)
        mmref.pack(padx=20, pady=10)
        ttk.Checkbutton(lower_frame, text='Migrar usuário (Cria novo usuário e migra todo o schema)', variable=self.migration_mode, onvalue=1).pack(padx=20, pady=10)
        ttk.Checkbutton(lower_frame, text='Migrar schema (Migra todo o schema sem criar novo usuário)', variable=self.migration_mode, onvalue=2).pack(padx=20, pady=10)
        mmref.invoke()
        ttk.Button(lower_frame, text='Cancelar', command=self.draw_start_menu).pack(side='left', padx=20, pady=10)
        ttk.Button(lower_frame, text='Prosseguir', command=lambda:self.pass_schema_selection(listbox)).pack(side='right', padx=20, pady=10)

    # Função auxiliar para a seleção de schema
    def pass_schema_selection(self, listbox):
        try:
            schema = listbox.get(listbox.curselection()[0])
            if schema:
                self.user = schema
            else:
                return
            # Migração de usuário/schema
            # Checa se usuário tem permissão para criar schemas
            if self.migration_mode.get() > 0 and self.mode == 'direct' and not self.pg_superuser:
                query = f"SELECT usename AS grantee, datname, privilege_type \
                        FROM pg_database, aclexplode(datacl) a \
                        JOIN pg_user e \
                        ON a.grantee = e.usesysid \
                        WHERE e.usename = '{self.pg_user}' AND datname = '{self.pg_database}'"
                perm = self.execute_query('pg', query)
                perm = [perm[i][2] for i in range(len(perm))]
                if 'CREATE' not in perm:
                    message = f"Usuário {self.pg_user} não tem permissão para criar o schema {self.pg_schema}\nLogue com um usuário com as devidas permissões!"
                    self.message_window(message)
                    self.write2log('Migração cancelada por falta de permissões')
                    if self.ora_dba:
                        self.draw_schema_selection_window()
                    else:
                        self.draw_table_selection_window(self.ora_user.upper())
                    return
            elif self.migration_mode.get() == 1:
                if self.mode == 'script':
                    self.migrate_user_window(schema, True)
                elif schema.lower() in self.pg_users:
                    self.user_exists(schema)
                else:
                    self.migrate_user_window(schema, True)
            elif self.migration_mode.get() == 2:
                self.migrate_schema_window(schema)
            # Migração de objetos
            else:
                self.draw_table_selection_window(schema)
        except Exception as e:
            print('Error: ' + str(e))

    # Pede permissão para deletar schema caso já exista
    def check_if_schema_exists(self, schema, objects):
        if schema.lower() == 'public':
            window = tk.Toplevel(self)
            ttk.Label(window, text="Inserir no schema padrão 'public'? (Schema public não pode ser deletado)").pack(padx=20, pady=20)
            ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=20)
            ttk.Button(window, text='Sim', command=lambda:[self.migrate_schema(schema, objects, 'keep'), window.destroy()]).pack(side='right', padx=20, pady=20)
        elif self.mode == 'direct':
            if schema.lower() in self.pg_schemas:
                window = tk.Toplevel(self)
                ttk.Label(window, text=f'O schema {schema.lower()} já existe na base de dados, como proceder?').pack(padx=20, pady=20)
                ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='right', padx=20, pady=20)
                ttk.Button(window, text='Deletar schema', command=lambda:[self.migrate_schema(schema, objects, 'delete'), window.destroy()]).pack(side='right', padx=20, pady=20)
                ttk.Button(window, text='Inserir no schema', command=lambda:[self.migrate_schema(schema, objects, 'keep'), window.destroy()]).pack(side='right', padx=20, pady=20)
        else:
            self.migrate_schema(schema, objects)

    def migrate_schema_window(self, schema):

        query = f"SELECT name, type FROM all_source WHERE owner = '{schema}' \
                UNION SELECT view_name, 'VIEW' FROM all_views WHERE owner = '{schema}' \
                UNION SELECT table_name, 'TABLE' FROM all_tables WHERE owner = '{schema}'"
        objects = self.execute_query('ora', query)
        objects = [[objects[i][0], objects[i][1]] for i in range(len(objects))]

        window = tk.Toplevel(self)
        ttk.Label(window, text='Objetos sendo migrados:').pack(padx=10, pady=10)
        box_frame = ttk.Frame(window)
        box_frame.pack()
        scrollBar = tk.Scrollbar(box_frame, orient='vertical')
        scrollBar.pack(side='right', fill='y')
        textBox = tk.Text(box_frame, yscrollcommand=scrollBar.set, state='normal', height=10)
        textBox.pack(pady=10)
        for object in objects:
            textBox.insert(tk.END, f'{object[0]}    {object[1]}\n')
        scrollBar.config(command=textBox.yview)
        textBox.config(state='disabled')

        ttk.Label(window, text="Nome do schema postgres: (Deixar vazio usará schema 'public')").pack(padx=10, pady=10)
        schema_entry = ttk.Entry(window, width=30)
        schema_entry.pack(padx=10)
        schema_entry.insert(0, schema)
        
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=20)
        ttk.Button(window, text='Prosseguir', command=lambda:[self.check_if_schema_exists(schema_entry.get(), objects), window.destroy()]).pack(side='right', padx=20, pady=20)

    # Coleta objetos do schema a ser migrado e cria novo schema
    def migrate_schema(self, schema, objects, mode=''):

        if schema == '':
            schema = 'public'

        if mode == 'delete':
            self.setup['delete_schema'].append(schema)
        elif mode == 'keep':
            if schema != 'public':
                self.setup['owner_schema'] = [self.pg_user, schema]
        else:
            self.setup['create_schema'] = schema

        self.pg_schema = schema.lower()
        tables = []
        sources = []
        for object in objects:
            if object[1] == 'TABLE':
                tables.append([schema, object[0]])
            else:
                sources.append([schema, object[0], object[1]])
        self.full_backup = True
        self.etl_initialization(tables, sources)
    
    # Pergunta o que fazer quando usuário já existe
    def user_exists(self, user):
        window = tk.Toplevel(self)
        ttk.Label(window, text=f'Usuário {user} já existe na base de dados!\nQual ação deseja tomar?').pack(padx=10, pady=10)
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
        ttk.Button(window, text='Deletar e prosseguir', command=lambda:[self.delete_user(user), window.destroy()]).pack(side='left', padx=20, pady=10)
        ttk.Button(window, text='Manter e prosseguir', command=lambda:[self.migrate_user_window(user, False), window.destroy()]).pack(side='left', padx=20, pady=10)

    # GUI para pegar configurações do novo usuário
    def migrate_user_window(self, user, new_user=True):
        window = tk.Toplevel(self)
        if new_user:
            ttk.Label(window, text=f'Digite uma senha para o novo usuário {user}:').pack(padx=10, pady=10)
        else:
            ttk.Label(window, text=f'Digite a senha do usuário {user}:').pack(padx=10, pady=10)
        password_entry = ttk.Entry(window, width=30)
        password_entry.pack(padx=10, pady=10)
        superuser = tk.BooleanVar()
        superuser.set(False)
        if new_user:
            su_cb = ttk.Checkbutton(window, text='Criar usuário como superuser', variable=superuser, onvalue=True, offvalue=False)
            su_cb.pack(padx=20, pady=10)
        create_db = tk.BooleanVar()
        c1 = ttk.Checkbutton(window, text='Migrar usuário para a base de dados conectada', variable=create_db, onvalue=False, tooltip='Cria o usuário nos conformes do Postgres, onde o usuário é algo abstrato e é dono de vários objetos\nNeste caso o usuário será dono de um schema com o mesmo nome que o seu')
        c1.pack(padx=10, pady=10)
        c2 = ttk.Checkbutton(window, text='Criar nova base de dados', variable=create_db, onvalue=True, tooltip="Preserva o contexto do Oracle, onde o usuário possui apenas 1 schema.\nNeste modo o usuário será dono do schema 'public' em um novo db")
        c2.pack(padx=10)
        c1.invoke()
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
        ttk.Button(window, text='Prosseguir', command=lambda:self.migrate_user(user, password_entry.get(), create_db.get(), superuser.get(), new_user, window)).pack(side='right', padx=20, pady=10)

    # Lista objetos que pertencem a um usuário a ser deletado
    def delete_user(self, user):
        if user == 'postgres':
            self.message_window('Você não pode deletar o usuário postgres!')
            return
        query = f"select nsp.nspname as SchemaName, cls.relname as ObjectName, rol.rolname as ObjectOwner, case cls.relkind \
                    when 'r' then 'TABLE' \
                    when 'm' then 'MATERIALIZED_VIEW' \
                    when 'i' then 'INDEX' \
                    when 'S' then 'SEQUENCE' \
                    when 'v' then 'VIEW' \
                    when 'c' then 'TYPE' \
                    else cls.relkind::text \
                end as ObjectType \
                from pg_class cls \
                join pg_roles rol \
                    on rol.oid = cls.relowner \
                join pg_namespace nsp \
                    on nsp.oid = cls.relnamespace \
                where nsp.nspname not in ('information_schema', 'pg_catalog') \
                    and nsp.nspname not like 'pg_toast%' \
                    and rol.rolname = '{user.lower()}' \
                order by nsp.nspname, cls.relname"
        objects = self.execute_query('pg', query)
        query = f"select datname from pg_database as db join pg_roles as role on db.datdba = role.oid where role.rolname = '{user.lower()}'"
        dbs = self.execute_query('pg', query)
        if len(objects) == 0 and len(dbs) == 0:
            self.migrate_user_window(user, True)
        else:
            window = tk.Toplevel(self)
            ttk.Label(window, text='Prosseguir com esta ação irá deletar os seguintes objetos:').pack(padx=10, pady=10)
            box_frame = ttk.Frame(window)
            box_frame.pack()
            scrollBar = tk.Scrollbar(box_frame, orient='vertical')
            scrollBar.pack(side='right', fill='y')
            textBox = tk.Text(box_frame, yscrollcommand=scrollBar.set, state='normal', height=10)
            textBox.pack(pady=10)
            if len(objects):
                textBox.insert(tk.END, 'Schema    Nome    Tipo\n')
                textBox.insert(tk.END, '-' * 30 + '\n')
                for object in objects:
                    textBox.insert(tk.END, f"{object[0]}    {object[1]}   {object[3]}\n")
                    self.deleted_objects.append([object[0], object[1], object[3]])
            textBox.insert(tk.END, '-' * 30 + '\n')
            if len(dbs):
                textBox.insert(tk.END, 'Base de dados\n')
                textBox.insert(tk.END, '-' * 30 + '\n')
                for db in dbs:
                    textBox.insert(tk.END, db[0] + '\n')
            scrollBar.config(command=textBox.yview)
            textBox.config(state='disabled')
            ttk.Label(window, text='Você tem certeza que deseja prosseguir?\n(Deleção ocorrerá apenas durante a migração)').pack(padx=10, pady=10)
            ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
            ttk.Button(window, text='Sim', command=lambda:[self.migrate_user_window(user, True), window.destroy()]).pack(side='right', padx=20, pady=10)

    # Executa a migração de usuário
    def migrate_user(self, user, password, create_db=False, superuser = False, new_user=True, window=None):
        if not user or not password:
            return
        user = user.lower()

        if self.mode == 'direct':
            # Checa se a senha fornecida está correta quando mantendo um usuário já existente
            if user.lower() in self.pg_users and not new_user:
                try:
                    test_conn = psycopg2.connect(dbname=self.pg_database, user=user.lower(), password=password, host=self.pg_host, port=self.pg_port)
                    test_conn.close()
                except:
                    self.message_window(f'Senha fornecida não é válida para o usuário {user.lower()}')
                    return
            else:
                window.destroy()
            cur = self.pg_conn.cursor()

            # Checa se usuário Postgres pode criar um db
            if create_db:
                query = f"SELECT usecreatedb FROM pg_user WHERE usename = '{self.pg_user}'"
                cur.execute(query)
                priv = cur.fetchone()
                if priv[0] == 'false':
                    self.message_window(f'Usuário {self.pg_user} não tem permissão para criar banco de dados!')
                    return

            if new_user:
                if user.lower() in self.pg_users:
                    # Agenda deleção de objetos e databases pertencentes ao usuário
                    query = f"select datname from pg_database as db join pg_roles as role on db.datdba = role.oid where role.rolname = '{user}'"
                    for db in self.pg_databases:
                        self.setup['delete_database'].append(db)
                    self.setup['delete_user'] = user

                if superuser:
                    self.setup['create_superuser'] = [user, password]
                else:
                    self.setup['create_user'] = [user, password]
        
        # Migra todas as tabelas e procedures relacionadas ao usuário
        query = f"SELECT table_name FROM all_tables WHERE owner = '{user.upper()}'"
        tables = self.etl.read.format('jdbc').options(driver=ORA_DRIVER, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        tables = [[user.upper(), tables[i]['TABLE_NAME']] for i in range(len(tables))]
        query = f"SELECT name, type FROM all_source where owner = '{user.upper()}' AND line = 1 UNION SELECT view_name, 'VIEW' FROM all_views where owner = '{user.upper()}'"
        sources = self.etl.read.format('jdbc').options(driver=ORA_DRIVER, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        sources = [[user.upper(), sources[i]['NAME'], sources[i]['TYPE']] for i in range(len(sources))]

        # Cria nova base de dados com o nome do usuário
        # Migrar usuário 'exemplo' criará o db 'exemplo' com o único schema 'public' e dará todas as permissões para o usuário 'exemplo' no db 'exemplo'
        if create_db:
            if self.mode == 'direct':
                # Checa se o database existe
                if user not in self.pg_databases:
                    self.setup['create_database'] = user
                else:
                    res = self.database_exists(user)
                    if res == 'delete':
                        self.setup['delete_database'].append(user)
                        self.setup['create_database'].append(user)
                    elif not res:
                        return
            else:
                self.setup['create_database'] = user
            self.setup['owner_database'] = [user, user]
            if self.mode == 'direct':
                new_pg_url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{user}"
            else:
                new_pg_url = ''
            self.setup['connect'] = {'url': new_pg_url, 'user': user, 'password': password, 'database': user}
            self.pg_schema = 'public'
        # Cria apenas o schema com o nome do usuário e insere no DB atual
        else:
            if self.mode == 'direct':
                pg_schemas = self.etl.read.format('jdbc').options(driver=PG_DRIVER, user=self.pg_user, password=self.pg_password, url=self.pg_url, query='SELECT nspname FROM pg_namespace').load().collect()
                pg_schemas = [pg_schemas[i]['nspname'] for i in range(len(pg_schemas))]
                if user not in pg_schemas:
                    self.setup['create_schema'] = user
            else:
                self.setup['create_schema'] = user
            self.setup['owner_schema'] = [user, user]
            self.pg_schema = user
            cur.close()
        self.full_backup = True
        self.etl_initialization(tables, sources)
    
    def database_exists(self, db):
        window = tk.Toplevel(self)
        res = tk.StringVar()
        res.set(None)
        ttk.Label(window, text=f'Database {db} já existe, como proceder?').pack(padx=10, pady=10)
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='right', padx=20, pady=20)
        ttk.Button(window, text='Deletar database', command=lambda:[res.set('delete'), window.destroy()]).pack(side='right', padx=20, pady=20)
        ttk.Button(window, text='Inserir no db existente', command=lambda:[res.set('keep'), window.destroy()]).pack(side='right', padx=20, pady=20)
        self.wait_window(window)
        return res.get()

    # Lista as tabelas disponiveis com a conexão estabelecida e permite o usuário escolher quais tabelas serão migradas
    def draw_table_selection_window(self, user):

        self.user = user

        for frame in self.winfo_children():
            frame.destroy()
        self.geometry('800x600')

        if self.ora_dba:
            query = f"SELECT table_name FROM all_tables WHERE owner = '{user}'"
        else:
            query = f"SELECT table_name FROM user_tables"
        tables = self.execute_query('ora', query)
        tables = [tables[i][0] for i in range(len(tables))]

        # Seleção de tabelas
        ttk.Label(self, text='Tabelas disponiveis para migração\nSelecione as tabelas que deseja migrar:').pack(padx=20, pady=10)
        box_frame = ttk.Frame(self, height=300)
        box_frame.pack(fill='x', expand=True)

        scrollBar = tk.Scrollbar(box_frame, orient='vertical')
        scrollBar.pack(side='right', fill='y')

        listbox = tk.Listbox(box_frame, selectmode='multiple', exportselection=False, yscrollcommand=scrollBar.set)
        listbox.pack(padx=10, pady=10, expand=True, fill='both')

        for table in tables:
            listbox.insert(tk.END, table)

        scrollBar.config(command=listbox.yview)

        ttk.Button(self, text='Selecionar todos', command=lambda: listbox.select_set(0, tk.END)).pack(anchor='w', padx=20)

        s_mode = tk.IntVar()
        user_migration = tk.BooleanVar()
        user_migration.set(False)

        c1 = ttk.Radiobutton(self, text='Migrar todos os procedimentos relacionados', variable=s_mode, value=0)
        c1.pack(padx=10)
        c2 = ttk.Radiobutton(self, text='Migrar todos os procedimentos na base de dados', variable=s_mode, value=1)
        c2.pack(padx=10)
        c3 = ttk.Radiobutton(self, text='Selecionar os procedimentos manualmente', variable=s_mode, value=2)
        c3.pack(padx=10)
        c1.invoke()

        ttk.Checkbutton(self, text=f'Migrar usuário (Cria usuário {user} em Postgres com suas devidas permissões)', variable=user_migration, onvalue=True, offvalue=False).pack(padx=10, pady=20)

        lower_frame = ttk.Frame(self)
        lower_frame.pack(expand=True)
        ttk.Label(lower_frame, text="Nome do schema para migração (Deixar vazio usará schema 'public')").pack(side='top', pady=20)
        self.schema_entry = ttk.Entry(lower_frame, width=30)
        self.schema_entry.pack()
        if self.ora_dba:
            ttk.Button(lower_frame, text='Voltar', command=self.draw_schema_selection_window).pack(side='left', padx=20, pady=20)
        else:
            ttk.Button(lower_frame, text='Cancelar', command=self.draw_start_menu).pack(side='left', padx=20, pady=20)
        if self.mode == 'direct':
            ttk.Button(lower_frame, text='Prosseguir', command=lambda: self.check_existing_tables(listbox, s_mode.get(), user_migration.get())).pack(side='right', padx=20, pady=20)
        else:
            ttk.Button(lower_frame, text='Prosseguir', command=lambda: self.source_selection_window(listbox, s_mode.get())).pack(side='right', padx=20, pady=20)

    # Checa se as tabelas a serem migradas já existem no schema
    def check_existing_tables(self, table_listbox, s_mode, user_migration):
        if len(table_listbox.curselection()) == 0 and s_mode == 0:
            self.message_window('Selecione algum objeto para migrar!')
            return
        self.pg_schema = self.schema_entry.get()
        if self.pg_schema == '':
            self.pg_schema = 'public'
        existing_tables = []
        tables = []
        pg_schemas = self.execute_query('pg', 'SELECT nspname FROM pg_namespace')
        pg_schemas = [pg_schemas[i][0] for i in range(len(pg_schemas))]
        # Checa se o schema existe no postgres
        if self.pg_schema not in pg_schemas:
            # Se o schema não existir, checa se o usuário tem permissão para criar schemas
            if not self.pg_superuser:
                query = f"SELECT usename AS grantee, datname, privilege_type \
                        FROM pg_database, aclexplode(datacl) a \
                        JOIN pg_user e \
                        ON a.grantee = e.usesysid \
                        WHERE e.usename = '{self.pg_user}' AND datname = '{self.pg_database}'"
                perm = self.execute_query('pg', query)
                perm = [perm[i][2] for i in range(len(perm))]
                if 'CREATE' not in perm:
                    message = f"Usuário {self.pg_user} não tem permissão para criar o schema {self.pg_schema}\nLogue com um usuário com as devidas permissões!"
                    self.message_window(message)
                    self.write2log('Migração cancelada por falta de permissões')
                    if self.ora_dba:
                        self.draw_schema_selection_window()
                    else:
                        self.draw_table_selection_window(self.ora_user.upper())
                    return
            self.setup['create_schema'] = self.pg_schema
        # Se o schema existir, checa se o usuário tem permissão para criar tabelas
        elif not self.pg_superuser:
            query = f"SELECT usename AS grantee, nspname, privilege_type \
                    FROM pg_namespace, aclexplode(nspacl) a \
                    JOIN pg_user e \
                    ON a.grantee = e.usesysid \
                    WHERE e.usename = '{self.pg_user}' and nspname = '{self.pg_schema}'"
            perm = self.execute_query('pg', query)
            perm = [perm[i][2] for i in range(len(perm))]
            if 'CREATE' not in perm:
                message = f'Usuário {self.pg_user} não tem permissão para criar tabelas no schema {self.pg_schema}!\nLogue com um usuário com as devidas permissôes!'
                self.message_window(message)
                self.write2log('Migração cancelada por falta de permissões')
                self.destroy()
                return
        # Coleta tabelas existentes no schema alvo
        query = f'''SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.pg_schema}' '''
        pg_tables = self.execute_query('pg', query)
        pg_tables = [pg_tables[i][0] for i in range(len(pg_tables))]
        for i in table_listbox.curselection():
            tables.append([self.user, table_listbox.get(i)])
        for table in tables:
            if table[1].lower() in pg_tables:
                existing_tables.append(table)
        if len(existing_tables) > 0:
            window = tk.Toplevel(self)
            window.geometry('600x400')
            ttk.Label(window, text='As seguintes tabelas já existem na base de dados alvo\nSelecione as tabelas que deseja substituir').pack(pady=10)
            list_frame = ttk.Frame(window, height=300)
            list_frame.pack(fill='x', expand=True)

            listbox_scrollBar = tk.Scrollbar(list_frame, orient='vertical')
            listbox_scrollBar.pack(side='right', fill='y')

            listbox = tk.Listbox(list_frame, selectmode='multiple', exportselection=False, yscrollcommand=listbox_scrollBar.set)
            listbox.pack(padx=10, pady=10, expand=True, fill='both')

            for table in existing_tables:
                listbox.insert(tk.END, table[1])
                self.deleted_objects.append([table[0], table[1], 'TABLE'])

            listbox_scrollBar.config(command=listbox.yview)

            ttk.Button(window, text='Selecionar todos', command=lambda: listbox.select_set(0, tk.END)).pack(anchor='w')
            lower_frame = ttk.Frame(window)
            lower_frame.pack(side='bottom', fill='x')
            ttk.Button(lower_frame, text='Cancelar', command=lambda: window.destroy()).pack(side='left', padx=10, pady=10)
            ttk.Button(lower_frame, text='Prosseguir', command=lambda: [self.update_tables(tables, pg_tables, listbox, s_mode), window.destroy()]).pack(side='right', padx=10, pady=10)
        else:
            self.source_selection_window(tables, s_mode)

    # Remove tabelas existentes que usuário não quer substituir
    def update_tables(self, tables, pg_tables, listbox, s_mode):
        override_tables = []
        for i in listbox.curselection():
            override_tables.append(listbox.get(i))
        aux_table = deepcopy(tables)
        for table in aux_table:
            # Se tabela já existe e não está marcada para substituição, remove da migração
            if table[1].lower() in pg_tables and table[1] not in override_tables:
                tables.remove(table)
        self.source_selection_window(tables, s_mode)

    # Idem com seleção de tabelas, mas agora para procedimentos guardados
    def source_selection_window(self, tables, s_mode):
        if type(tables) != list:
            aux = tables
            tables = []
            for i in aux.curselection():
                tables.append([self.user, aux.get(i)])
            self.pg_schema = self.schema_entry.get()
            if self.pg_schema == '':
                self.pg_schema = 'public'
        if s_mode < 2:
            window = tk.Toplevel(self)
            window.geometry('600x400')
            ttk.Label(window, text='Procedimentos sendo migrados:').pack(padx=20, pady=10)
            source_frame = ttk.Frame(window, height=300)
            source_frame.pack(fill='x', expand=True)

            source_scrollBar = tk.Scrollbar(source_frame, orient='vertical')
            source_scrollBar.pack(side='right', fill='y')

            textbox = tk.Text(source_frame, yscrollcommand=source_scrollBar.set, state='normal', height=10)
            textbox.pack(expand=True, fill='both')

            # Migra procedimentos relacionados
            if s_mode == 0:
                sources = []
                for table in tables:
                    query = f"SELECT DISTINCT owner, name, type FROM all_dependencies WHERE referenced_name = '{table[1]}' AND owner = '{table[0]}'"
                    dependencies = self.execute_query('ora', query)
                    for i in range(len(dependencies)):
                        sources.append([dependencies[i][0], dependencies[i][1], dependencies[i][2]])
                        self.deleted_objects.append([dependencies[i][0], dependencies[i][1], dependencies[i][2]])
                        textbox.insert(tk.END, f"{dependencies[i][0]}.{dependencies[i][1]}    {dependencies[i][2]}\n")
            
            # Migra todos os procedimentos
            elif s_mode == 1:
                query = f"SELECT DISTINCT owner, name, type FROM all_dependencies WHERE owner = '{self.user}'"
                sources = self.execute_query('ora', query)
                sources = [[sources[i][0], sources[i][1], sources[i][2]] for i in range(len(sources))]
                for source in sources:
                    textbox.insert(tk.END, f'{source[0]}.{source[1]}    {source[2]}\n')

            if len(sources) == 0:
                window.destroy()
                return self.etl_initialization(tables, sources)

            source_scrollBar.config(command=textbox.yview)
            textbox.config(state='disabled')

            lower_frame = ttk.Frame(window)
            lower_frame.pack()
            ttk.Button(lower_frame, text='Cancelar', command=lambda: window.destroy()).pack(side='left', padx=20, pady=10)
            ttk.Button(lower_frame, text='Concordar', command=lambda: [self.etl_initialization(tables, sources), window.destroy()]).pack(side='right', padx=20, pady=10)

        # Seleciona manualmente procedimentos a serem migrados
        elif s_mode == 2:
            window = tk.Toplevel(self)
            window.geometry('600x400')
            query = f"SELECT name, type FROM all_source where owner = '{self.user}' AND line = 1 UNION SELECT view_name, 'VIEW' FROM all_views where owner = '{self.user}'"
            sources = self.execute_query('ora', query)
            sources = [[self.user, sources[i][0], sources[i][1]] for i in range(len(sources))]

            ttk.Label(window, text='Procedimentos disponiveis para migração\nSelecione quais deseja migrar:').pack(padx=20, pady=10)
            source_frame = ttk.Frame(window, height=300)
            source_frame.pack(fill='x', expand=True)

            source_scrollBar = tk.Scrollbar(source_frame, orient='vertical')
            source_scrollBar.pack(side='right', fill='y')

            source_listbox = tk.Listbox(source_frame, selectmode='multiple', yscrollcommand=source_scrollBar.set)
            source_listbox.pack(expand=True, fill='both')

            for src in sources:
                source_listbox.insert(tk.END, f"{src[0]}.{src[1]}   {src[2]}")

            source_scrollBar.config(command=source_listbox.yview)

            lower_frame = ttk.Frame(window)
            lower_frame.pack()
            ttk.Button(lower_frame, text='Cancelar', command=lambda: window.destroy()).pack(side='left', padx=20, pady=10)
            ttk.Button(lower_frame, text='Prosseguir', command=lambda: [self.etl_initialization(tables, source_listbox), window.destroy()]).pack(side='right', padx=20, pady=10)

    # Coleta os nomes dos objetos selecionados pelo usuário e detecta dependencias
    def etl_initialization(self, tables, sources):
        if len(tables) == 0 and type(sources) != list:
            if len(sources.curselection()) == 0:
                window = tk.Toplevel(self)
                ttk.Label(window, text='Nenhuma tabela ou procedimento selecionado\nRetornando ao menu inicial').pack(padx=10, pady=10)
                ttk.Button(window, text='Ok', command=window.destroy).pack(pady=10)
                self.wait_window(window)
                if self.ora_dba:
                    self.draw_schema_selection_window()
                else:
                    self.draw_table_selection_window(self.ora_user.upper())
                return
        if type(sources) != list:
            selected_sources = []
            for i in sources.curselection():
                split_src = sources.get(i).split()
                src_type = split_src[1]
                split_src = split_src[0].split('.')
                selected_sources.append([split_src[0], split_src[1], src_type])
        else:
            selected_sources = sources
        dep_tables = set()
        # Detecta dependencias entre tabelas
        for table in tables:
            query = f"SELECT DISTINCT table_name, owner FROM all_cons_columns WHERE constraint_name in \
            (SELECT r_constraint_name FROM all_constraints WHERE constraint_type = 'R' AND table_name = '{table[1]}' AND owner = '{table[0]}')"
            r_tables = self.execute_query('ora', query)
            for i in range(len(r_tables)):
                r_owner = r_tables[i][1]
                r_name = r_tables[i][0]
                # Checa se o usuário tem acesso a uma tabela que não o pertence
                if r_owner != self.user.upper():
                    try:
                        query = f"SELECT * FROM {r_owner}.{r_name} WHERE rownum = 1"
                        res = self.execute_query('ora', query)
                        if len(res) == 0:
                            raise Exception
                    except:
                        self.message_window(f'Usuário {self.user} Não tem acesso à tabela {r_owner}.{r_name}, migração cancelada!')
                        return
            r_tables = [[r_tables[i][1], r_tables[i][0]] for i in range(len(r_tables))]
            for tab in r_tables:
                if tab not in tables:
                    dep_tables.add(tuple(tab))
        dep_sources = set()
        if sources:
            # Detecta dependencias entre procedimentos
            for src in selected_sources:
                query = f"SELECT DISTINCT referenced_name, referenced_type, referenced_owner FROM all_dependencies WHERE name = '{src[1]}' \
                        AND owner = '{src[0]}' AND referenced_type not in ('PACKAGE') AND referenced_owner != 'PUBLIC' AND referenced_owner not like '%SYS%'"
                dependencies = self.execute_query('ora', query)
                # Checa se o usuário tem acesso a um objeto que não o pertence
                for i in range(len(dependencies)):
                    r_owner = dependencies[i][2]
                    r_type = dependencies[i][1]
                    r_name = dependencies[i][0]
                    if r_owner != self.user.upper():
                        try:
                            if r_type in ['TABLE', 'VIEW']:
                                query = f"SELECT * FROM {r_owner}.{r_name} WHERE rownum = 1"
                            else:
                                query = f"SELECT * FROM all_source WHERE owner = '{r_owner}' AND name = '{r_name}' AND line = 1"
                            res = self.execute_query('ora', query)
                            if len(res) == 0:
                                raise Exception
                        except:
                            self.message_window(f'Usuário {self.user} não tem acesso ao {r_type} {r_name}\n \
                                                referenciado por {src}, migração cancelada!')
                            return
                dependencies = [[dependencies[i][2], dependencies[i][0], dependencies[i][1]] for i in range(len(dependencies))]
                # Detecta se procedimento depende de algum objeto
                for dep in dependencies:
                    if dep[2] == 'TABLE':
                        if [dep[0], dep[1]] not in tables:
                            dep_tables.add((dep[0], dep[1]))
                    elif dep[2] == 'SEQUENCE' and dep not in selected_sources:
                        selected_sources.append(dep)
                    elif dep not in selected_sources:
                        dep_sources.add(tuple(dep))
            dep_sources = list(dep_sources)
        dep_tables = list(dep_tables)
        if len(dep_tables) > 0 or len(dep_sources) > 0:
            self.draw_dep_window(dep_tables, tables, dep_sources, selected_sources)
        elif self.mode == 'direct':
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.pg_schema}'"
            pg_tables = self.execute_query('pg', query)
            for table in pg_tables:
                if [self.pg_schema, table[0]] in tables:
                    self.deleted_objects.append([self.pg_schema, table['table_name'], 'TABLE'])
            # Adiciona sources já existentes na base alvo para a lista de objetos removidos
            for source in selected_sources:
                query = f"select proname, 'PROC/FUNC' as type from pg_proc where proname = '{source[1]}' \
                        union select viewname, 'VIEW' from pg_views where viewname = '{source[1]}' \
                        union select tgname, 'TRIGGER' from pg_trigger where tgname = '{source[1]}'"
                res = self.execute_query('pg', query)
                if len(res) > 0:
                    found = False
                    for object in self.deleted_objects:
                        if res[0][0] == object[1]:
                            found = True
                            break
                    if not found:
                        self.deleted_objects.append([self.pg_schema, res[0][0], res[0][1]])
            self.select_data_window(tables, selected_sources)
        else:
            self.select_data_window(tables, selected_sources)

    # Lista dependencias entre os objetos escolhidos e pede permissão para incluí-los à migração
    def draw_dep_window(self, dep_tables, sel_tables, dep_sources, sel_sources):
        window = tk.Toplevel(self)
        ttk.Label(window, text='Algumas das tabelas/procedimentos selecionados dependem das seguintes tabelas/procedimentos\nPara prosseguir elas tambem serão migradas').pack(padx=10, pady=10)
        box_frame1 = ttk.Frame(window)
        box_frame1.pack(padx=10, pady=10)
        scrollBar = tk.Scrollbar(box_frame1, orient='vertical')
        scrollBar.pack(side='right', fill='y')
        textBox = tk.Text(box_frame1, yscrollcommand=scrollBar.set, state='normal', height=10)
        textBox.pack(pady=10)
        for table in dep_tables:
            textBox.insert(tk.END, f'{table[0]}.{table[1]}    TABLE\n')
            sel_tables.append(list(table))
        for src in dep_sources:
            textBox.insert(tk.END, f'{src[0]}.{src[1]}    {src[2]}\n')
            sel_sources.append(list(src))
        textBox.config(state='disabled')
        lower_frame = ttk.Frame(window)
        lower_frame.pack()
        ttk.Button(lower_frame, text='Concordar', command=lambda: [self.select_data_window(sel_tables, sel_sources), window.destroy()]).pack(side='right', padx=20, pady=10)
        ttk.Button(lower_frame, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)

    # Lista as tabelas da migração e permite filtrar quais dados seram migrados
    def select_data_window(self, tables, sources):
        self.table_data = {}
        window = tk.Toplevel(self)
        ttk.Label(window, text='Selecione como os dados de cada tabela devem ser migrados:').pack(padx=10, pady=10)
        
        # Frame scrollavel para widgets
        outerScrollFrame = tk.Frame(window)
        outerScrollFrame.pack(fill='x', expand=True)
        scrollBar = ttk.Scrollbar(outerScrollFrame, orient='vertical')
        scrollBar.pack(side='right', fill='y')
        canvas = tk.Canvas(outerScrollFrame)
        canvas.pack(fill='x', expand=True)

        scrollBar.configure(command=canvas.yview)
        canvas.configure(yscrollcommand=scrollBar.set)

        innerScrollFrame = tk.Frame(canvas, bg='white')
        innerScrollFrame.pack(fill='both', expand=True)
        innerScrollFrame.bind('<Configure>', lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        # ???
        frame = canvas.create_window((0, 0), window=innerScrollFrame, anchor='nw')

        max_label_size = 0

        for i, table in enumerate(tables):
            table_name = deepcopy(table[1])
            schema = deepcopy(table[0])
            self.table_data[f'{schema}.{table_name}'] = {}
            ttk.Label(innerScrollFrame, text=f'{schema}.{table_name}', background='white').grid(row=i, column=0, padx=5)
            self.table_data[f'{schema}.{table_name}']['config'] = tk.StringVar()
            self.table_data[f'{schema}.{table_name}']['filter'] = {}
            rbref = ttk.Radiobutton(innerScrollFrame, text='Todos os dados', variable=self.table_data[f'{schema}.{table_name}']['config'], value='all', style='White.TRadiobutton')
            rbref.grid(row=i, column=1, padx=5)
            ttk.Radiobutton(innerScrollFrame, text='Apenas esqueleto', variable=self.table_data[f'{schema}.{table_name}']['config'], value='none', style='White.TRadiobutton').grid(row=i, column=2, padx=5)
            ttk.Radiobutton(innerScrollFrame, text='Customizado', variable=self.table_data[f'{schema}.{table_name}']['config'], value='custom', command=lambda table_name=table_name, schema=schema:self.customize_column_data(schema, table_name), style='White.TRadiobutton').grid(row=i, column=3, padx=5)
            rbref.invoke()
            cur_label_size = len(f'{schema}.{table_name}')
            if cur_label_size > max_label_size:
                max_label_size = cur_label_size

        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=20)
        if self.mode == 'direct':
            ttk.Button(window, text='Prosseguir', command=lambda:[self.list_objects(tables, sources), window.destroy()]).pack(side='right', padx=20, pady=20)
        else:
            ttk.Button(window, text='Prosseguir', command=lambda:[self.begin_etl_session(tables, sources), window.destroy()]).pack(side='right', padx=20, pady=20)

        # Redimensionamento manual da janela pois o organizador automático não está funcionando
        width = 400 + max_label_size * 10
        window.geometry(f'{width}x400')
        window.update()
        canvas.update()

    # Customiza quais dados de colunas especificas devem ser migrados
    def customize_column_data(self, schema, table):
        query = f"SELECT column_name, data_type FROM all_tab_columns \
                WHERE owner = '{schema}' AND table_name = '{table}'"
        res = self.execute_query('ora', query)
        columns = [[res[i][0], res[i][1]] for i in range(len(res))]

        window = tk.Toplevel(self)
        ttk.Label(window, text=f'Defina como filtrar os dados\nColunas da tabela {schema}.{table}:').pack(padx=10, pady=10)
        
        outerScrollFrame = tk.Frame(window)
        outerScrollFrame.pack(fill='x', expand=True)
        scrollBar = ttk.Scrollbar(outerScrollFrame, orient='vertical')
        scrollBar.pack(side='right', fill='y')
        canvas = tk.Canvas(outerScrollFrame)
        canvas.pack(fill='x', expand=True)
        scrollBar.configure(command=canvas.yview)
        canvas.configure(yscrollcommand=scrollBar.set)

        innerScrollFrame = tk.Frame(canvas, bg='white')
        innerScrollFrame.pack(fill='both', expand=True)
        innerScrollFrame.bind('<Configure>', lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        frame = canvas.create_window((0, 0), window=innerScrollFrame, anchor='nw')

        operators = {}
        widgets = {}

        max_label_size = 0

        for i, column in enumerate(columns):
            widgets[column[0]] = {}
            ttk.Label(innerScrollFrame, text=column[0], background='white').grid(row=i, column=0)
            if column[1] in ('NUMBER', 'INTEGER', 'LONG', 'FLOAT', 'DATE', 'TIMESTAMP', 'RAW'):
                operators[column[0]] = tk.StringVar()
                widgets[column[0]]['operator'] = ttk.Combobox(innerScrollFrame, textvariable=operators[column[0]], state='readonly', values=('None', '=', '!=', '>', '<', '>=', '<=', 'BETWEEN'))
                widgets[column[0]]['operator'].current(0)
                widgets[column[0]]['operator'].grid(row=i, column=1)
            elif column[1] in ('VARCHAR', 'VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR'):
                operators[column[0]] = tk.StringVar()
                widgets[column[0]]['operator'] = ttk.Combobox(innerScrollFrame, textvariable=operators[column[0]], state='readonly', values=('None', '=', '!=', 'LIKE', 'NOT LIKE'))
                widgets[column[0]]['operator'].current(0)
                widgets[column[0]]['operator'].grid(row=i, column=1)
            widgets[column[0]]['condition'] = ttk.Entry(innerScrollFrame, width=20, state='disabled', tooltip="Caso deseje filtar com BETWEEN, insira os valores de filtragem no formato 'limiteMenor-limiteMaior' separados com - sem espaço!")
            widgets[column[0]]['condition'].grid(row=i, column=2)
            widgets[column[0]]['operator'].bind('<<ComboboxSelected>>', lambda event, c = column[0]: self.check_variable_event(operators[c], widgets[c]['condition'], event))
            if len(column[0]) > max_label_size:
                max_label_size = len(column[0])
        ttk.Button(window, text='Testar filtro', command=lambda: self.test_custom_filter(schema, table, columns, operators, widgets, result_display)).pack(padx=20, pady=10)
        result_display = ScrolledText(window, wrap=tk.WORD, state='disabled')
        result_display.config(height=10, width=30)
        result_display.pack(pady=10, fill='x', expand=True)
        ttk.Button(window, text='Salvar', command=lambda:[self.save_configuration(schema, table, columns, operators, widgets), window.destroy()]).pack(padx=20, pady=10)
        self.load_configuration(schema, table, columns, widgets)
        width = 400 + max_label_size * 10
        window.geometry(f'{width}x650')
        canvas.update()
        window.update()

    def check_variable_event(self, var, entry, event):
        if var.get() != 'None':
            entry.config(state = 'normal')
        else:
            entry.delete(0, tk.END)
            entry.config(state = 'disabled')

    def load_configuration(self, schema, table, columns, widgets):
        try:
            if self.table_data[f'{schema}.{table}']['config'].get() == 'custom':
                for column in columns:
                    if len(self.table_data[f'{schema}.{table}']['filter'].keys()) > 0:
                        operator = self.table_data[f'{schema}.{table}']['filter'][column[0]]['operator']
                        op_index = self.table_data[f'{schema}.{table}']['filter'][column[0]]['operator_index']
                        if operator != 'None':
                            value = self.table_data[f'{schema}.{table}']['filter'][column[0]]['value']
                            widgets[column[0]]['operator'].current(op_index)
                            widgets[column[0]]['condition'].config(state='normal')
                            widgets[column[0]]['condition'].insert(0, value)
        except Exception as e:
            print(e)
            return

    def save_configuration(self, schema, table, columns, operators, widgets):
        for column in columns:
            if operators[column[0]].get() != 'None':
                value = widgets[column[0]]['condition'].get()
                # Proteção contra injeção de sql
                if len(value.split()) == 1:
                    self.table_data[f'{schema}.{table}']['filter'][column[0]] = {}
                    self.table_data[f'{schema}.{table}']['filter'][column[0]]['operator'] = operators[column[0]].get()
                    self.table_data[f'{schema}.{table}']['filter'][column[0]]['operator_index'] = widgets[column[0]]['operator'].current()
                    self.table_data[f'{schema}.{table}']['filter'][column[0]]['value'] = value
                else:
                    self.message_window('Por favor, não utilize espaços nas entradas de filtragem!')
                    return
            else:
                self.table_data[f'{schema}.{table}']['filter'][column[0]] = {}
                self.table_data[f'{schema}.{table}']['filter'][column[0]]['operator'] = 'None'

    # Testa integridade da filtragem
    def test_custom_filter(self, schema, table, columns, operators, widgets, display):
        filter = ''
        for column in columns:
            if operators[column[0]].get() != 'None':
                value = widgets[column[0]]['condition'].get()
                # Proteção contra injeção de sql
                if len(value.split()) == 1:
                    if operators[column[0]].get() == 'BETWEEN':
                        if '-' not in value:
                            self.message_window('Formato incorreto, leia a descrição no tooltip!')
                            return
                        limits = value.split('-')
                        value = f'{limits[0]} AND {limits[1]}'
                    if filter == '':
                        filter = f"WHERE {column[0]} {operators[column[0]].get()} {value}"
                    else:
                        filter += f"\nAND {column[0]} {operators[column[0]].get()} {value}"
                else:
                    self.message_window('Por favor, não utilize espaços nas entradas de filtragem!')
                    return
        query = f'SELECT * FROM {schema}.{table} {filter}'
        try:
            res = self.execute_query('ora', query)
            display.config(state='normal')
            display.delete('1.0', tk.END)
            if res:
                for data in res:
                    display.insert(tk.END, str(data) + '\n')
            display.config(state='disabled')
            return
        except Exception as e:
            self.message_window('Execução da query falhou, verifique se os valores de filtragem estão bem definidos!')
            print(e)
            return

    # Lista tudo que será inserido e tudo que será deletado no banco de dados alvo
    def list_objects(self, tables, sources):
        window = tk.Toplevel(self)
        ttk.Label(window, text='Os seguintes objetos serão criádos/inseridos na base de dados:').pack(padx=10, pady=10)
        added_info = ScrolledText(window, wrap=tk.WORD, state='normal')
        added_info.config(height=20, width=50)
        added_info.pack(padx=10, pady=10)
        if self.setup['create_database']:
            added_info.insert(tk.END, f"Cria novo database {self.setup['create_database']}\n")
        if self.setup['create_schema']:
            added_info.insert(tk.END, f"Cria novo schema {self.setup['create_schema']}\n")
        if self.setup['create_user']:
            added_info.insert(tk.END, f"Cria novo usuário {self.setup['create_user']}\n")
        for table in tables:
            added_info.insert(tk.END, f'Tabela {table[0]}.{table[1]} Oracle -> {self.pg_schema}.{table[1]} Postgres\n')
        for source in sources:
            added_info.insert(tk.END, f'{source[2]} {source[0]}.{source[1]} Oracle -> {self.pg_schema}.{source[1]} Postgres\n')
        if len(self.setup['delete_database']) > 0 or len(self.setup['delete_schema']) > 0 or len(self.deleted_objects) > 0 or self.setup['delete_user']:
            ttk.Label(window, text='Os seguintes objetos serão removidos da base de dados:').pack(padx=10, pady=10)
            dropped_info = ScrolledText(window, wrap=tk.WORD, state='normal')
            dropped_info.config(height=20, width=50)
            dropped_info.pack(padx=10, pady=10)
            for database in self.setup['delete_database']:
                dropped_info.insert(tk.END, f'Remove database {database}\n')
            for schema in self.setup['delete_schema']:
                dropped_info.insert(tk.END, f'Remove schema {schema}\n')
            if self.setup['delete_user']:
                dropped_info.insert(tk.END, f"Deleta usuário {self.setup['create_user']}\n")
            for object in self.deleted_objects:
                dropped_info.insert(tk.END, f'Substitui {object[2]} {object[0]}.{object[1]}\n')
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=20)
        ttk.Button(window, text='Começar a migrar!', command=lambda:[self.begin_etl_session(tables, sources), window.destroy()]).pack(side='right', padx=20, pady=20)

    # Inicializa a sessão de etl e começa a migrar
    @threaded
    def begin_etl_session(self, tables, sources):
        pg_conf = {'host': self.pg_host, 'port': self.pg_port,'user': self.pg_user, 'password': self.pg_password, 'database': self.pg_database, 'url': self.pg_url}
        ora_conf = {'host': self.ora_host, 'port': self.ora_port, 'user': self.ora_user, 'password': self.ora_password, 'service': self.ora_service, 'url': self.ora_url}
        if self.mode == 'direct':
            if not self.full_backup:
                self.write2log(f'Criando backup do schema {self.pg_schema}')
                if self.create_backup(self.pg_schema):
                    pass
                else:
                    return
            else:
                self.write2log(f'Criando backup do database {self.pg_database}')
                if self.create_backup():
                    pass
                else:
                    return
            etl_session = Oracle2PostgresETL(self.master, pg_conf, ora_conf, self.user, tables, sources, self.etl, self.pg_jar, self.ora_jar, self.pg_schema, self.setup, self.table_data, 'direct')
            self.write2log(f"migrando tabelas {tables} e sources {sources} do Oracle schema '{self.user}' para Postgres database {self.pg_database} schema '{self.pg_schema}'!")
            self.active_sessions.append(etl_session)
            # Começa a migração
            etl_session.setup_processing_direct()
            etl_session.start_etl()
        elif self.mode == 'script':
            etl_session = Oracle2PostgresETL(self.master, pg_conf, ora_conf, self.user, tables, sources, self.etl, self.pg_jar, self.ora_jar, self.pg_schema, self.setup, self.table_data, 'script')
            self.write2log(f'Gerando script para migração de Oracle para Postgres')
            self.active_sessions.append(etl_session)
        self.wait4ETL(etl_session)
        if self.ora_dba:
            self.draw_schema_selection_window()
        else:
            self.draw_table_selection_window(self.ora_user.upper())

    # Espera processo terminar e restaura para o estado anterior em caso de falha
    def wait4ETL(self, session):
        # Sei que isso é feio mas não consegui fazer funcionar de outra forma
        # caso encontre um método mais elegante para esperar o processo, sinta-se livre para substituir
        while session._state == 'idle' or session._state == 'executing':
            sleep(1)
        if self.mode == 'direct':
            if session._state == 'success':
                self.write2log('Migração concluída com sucesso!')
            elif session._state == 'failed':
                self.write2log(f'Migração falhou!\nFalha: {session.error_message}\nRetornando base de dados para estado anterior...')
                if self.full_backup:
                    self.restore_from_backup()
                else:
                    self.restore_from_backup(self.pg_schema)
                self.write2log('Base de dados retornada ao último estado estável')
            if self.full_backup:
                self.remove_backup()
            else:
                self.remove_backup(self.pg_schema)
        elif self.mode == 'script':
            if session._state == 'success':
                self.write2log('Script criado com sucesso!')
            elif session._state == 'failed':
                self.write2log(f'Criação do script falhou!\nFalha: {session.error_message}')
        self.active_sessions.remove(session)
        self.write2log(f'Terminada sessão de ETL {self.etl.__hash__()}')
        del session

# Vice versa
class Postgres2Oracle(tk.Toplevel):
    def __init__(self, master, conn_session):
        super().__init__(master)
        self.protocol('WM_DELETE_WINDOW', self.close)
        self.title('Postgres para Oracle')
        self_refs = dir(self)
        self.etl = conn_session
        for key in dir(master):
            if key[0] != '_' and key not in self_refs:
                exec(f'self.{key} = master.{key}')
        self.master = master
        self.draw_schema_selection_window()

    def close(self):
        print('proper closure')
        self.etl.stop()
        self.destroy()

    def execute_query(self, type, query):
        try:
            if type == 'pg':
                cur = self.pg_conn.cursor()
                cur.execute(query)
                res = cur.fetchall()
                cur.close()
            elif type == 'ora':
                cur = self.ora_conn.cursor()
                cur.execute(query)
                res = cur.fetchall()
                cur.close()
            return res
        except Exception as e:
            print('Error' + str(e))

    def create_backup(self, schema=None):
        try:
            check = Popen('expdp', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            check.wait()
            if self.ora_host not in ['localhost', '127.0.0.1', '::1']:
                self.local_param = self.get_local_oracle_connection()
                if not self.local_param:
                    return
            self.local_url = f"jdbc:oracle:thin:@//localhost:{self.local_param[1]}/{self.local_param[2]}"
            self.local_conn = jaydebeapi.connect(self.ora_driver, self.local_url, ['sys as dba', self.local_param[0]], self.ora_jar)
            BACKUP_DIR = os.path.join(APP_HOME, f'backups')
            cur = self.local_conn.cursor()
            cur.execute(f'CREATE OR REPLACE DIRECTORY backup_dir AS {BACKUP_DIR}')
            cur.execute('SELECT DB_LINK FROM ALL_DB_LINKS')
            links = cur.fetchall()
            links = [links[i][0] for i in range(len(links))]
            if 'BACKUP_LINK' not in links:
                cur.execute(f"CREATE DATABASE LINK BACKUP_LINK CONNECT TO {self.ora_user} IDENTIFIED BY '{self.ora_password}' USING \
                            (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.ora_host})(PORT={self.ora_port})(CONNECT_DATA=(SERVICE_NAME={self.ora_service}))))")
            if schema:
                command = f"expdp sys/{self.local_param[0]}@{self.local_param[2]} schemas={schema} directory=backup_dir dumpfile={schema}_backup.dmp logfile={schema}_backup.log"
            else:
                command = f"expdp sys/{self.local_param[0]}@{self.local_param[2]} full=Y directory=backup_dir dumpfile={self.ora_service}_backup.dmp logfile={self.ora_service}_backup.log"
            window = tk.Toplevel(self)
            label = ttk.Label(window, text='Fazendo backup da base de dados')
            label.pack(padx=10, pady=10)
            ttk.Button(window, text='Cancelar', command=self.close).pack(padx=10, pady=10)
            self.backup_window(window, label)
            p = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            p.wait()
            window.destroy()
            cur.close()
            return True
        except:
            self.write2log('datapump não instalado, backup não efetuado!')
            if window.winfo_exists():
                window.destroy()
            window = tk.Toplevel(self)
            var = tk.BooleanVar()
            var.set(False)
            ttk.Label(window, text='datapump não encontrado, prosseguir sem fazer backup?\n(Para realizar o backup, por favor instale o oracle versão 11 ou maior nesta máquina)')
            ttk.Button(window, text='Não', command=window.destroy).pack(side='left', padx=20, pady=20)
            ttk.Button(window, text='Sim', command=lambda:[var.set(True), window.destroy()]).pack(side='right', padx=20, pady=20)
            self.wait_window(window)
            return var.get()

    def get_local_oracle_connection(self):
        window = tk.Toplevel(self)
        ttk.Label(window, text='Para realizar um backup em uma conexão remota, é necessário acessar o oracle local!').pack(padx=10, pady=10)
        ttk.Label(window, text='Digite a senha do usuário sys local:').pack(padx=10, pady=10)
        password_entry = ttk.Entry(window, width=30)
        password_entry.pack(padx=10, pady=10)
        ttk.Label(window, text='Digite a porta do serviço oracle:').pack(padx=10, pady=10)
        port_entry = tk.Entry(window, width=30)
        port_entry.pack(padx=10, pady=10)
        ttk.Label(window, text='Digite o nome do serviço do oracle local:').pack(padx=10, pady=10)
        service_entry = ttk.Entry(window, width=30)
        service_entry.pack(padx=10, pady=10)
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=10, pady=10)
        ttk.Button(window, text='Prosseguir', command=lambda:[password.set(password_entry.get()), port.set(port_entry.get()), service.set(service_entry.get()), window.destory()]).pack(side='right', padx=10, pady=10)
        password = tk.StringVar()
        port = tk.StringVar()
        service = tk.StringVar()
        self.wait_window(window)
        return [password.get(), port.get(), service.get()]

    @threaded
    # Pequena animação enquanto o backup ocorre
    def backup_window(self, window, label):
        while window.winfo_exists():
            sleep(0.5)
            if label.cget("text") == 'Fazendo backup da base de dados...':
                label['text'] = 'Fazendo backup da base de dados'
            else:
                label['text'] += '.'

    def restore_from_backup(self, schema=None):
        if schema:
            command = f"impdp sys/{self.local_param[0]}@{self.local_param[2]} schemas={schema} directory=backup_dir dumpfile={schema}_backup.dmp logfile={schema}_backup.log"
        else:
            command = f"impdp sys/{self.local_param[0]}@{self.local_param[2]} full=Y directory=backup_dir dumpfile={self.ora_service}_backup.dmp logfile={self.ora_service}_backup.log"
        p = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        p.wait()

    def remove_backup(self, schema=None):
        if schema:
            BACKUP_FILE = os.path.join(APP_HOME, f'backups/{schema}_backup.dmp')
            LOGS_FILE = os.path.join(APP_HOME, f'backups/{schema}_backup.log')
        else:
            BACKUP_FILE = os.path.join(APP_HOME, f'backups/{self.ora_service}_backup.dmp')
            LOGS_FILE = os.path.join(APP_HOME, f'backups/{self.ora_service}_backup.log')
        try:
            os.remove(BACKUP_FILE)
            os.remove(LOGS_FILE)
        except:
            self.write2log("Arquivo de backup não encontrado, prosseguindo...")

    # Lista schemas visíveis pelo usuário logado
    def draw_schema_selection_window(self):
        for frame in self.winfo_children():
           frame.destroy()
        self.geometry('800x600')

        # Coleta schemas da base de dados atual aos quais o usuário tem acesso
        query = "SELECT nspname FROM pg_namespace ns WHERE pg_catalog.has_schema_privilege(current_user, ns.nspname, 'USAGE') = 'true' \
                AND ns.nspname != 'information_schema' AND ns.nspname not like 'pg_%'"
        schemas = self.execute_query('pg', query)
        schemas = [schemas[i][0] for i in range(len(schemas))]

        ttk.Label(self, text='Selecione o schema que deseja migrar').pack(padx=20, pady=10)
        user_frame = ttk.Frame(self, height=300)
        user_frame.pack(fill='x', expand=True)

        scrollBar = tk.Scrollbar(user_frame, orient='vertical')
        scrollBar.pack(side='right', fill='y')

        listbox = tk.Listbox(user_frame, selectmode='single', exportselection=False, yscrollcommand=scrollBar.set)
        listbox.pack(padx=10, pady=10, expand=True, fill='both')

        for schema in schemas:
            listbox.insert(tk.END, schema)

        scrollBar.config(command=listbox.yview)

        self.migration_mode = tk.IntVar()

        lower_frame = ttk.Frame(self)
        lower_frame.pack()
        mmref = ttk.Checkbutton(lower_frame, text='Migrar tabelas (Migra objetos do schema selecionado para o schema Oracle conectado)', variable=self.migration_mode, onvalue=0, command=lambda:listbox.config(state='normal'))
        mmref.pack(padx=20, pady=20)
        ttk.Checkbutton(lower_frame, text='Migrar schema (Migra todos os objetos do schema selecionado para um schema Oracle)', variable=self.migration_mode, onvalue=1, command=lambda:listbox.config(state='normal')).pack(padx=20, pady=20)
        ttk.Checkbutton(lower_frame, text='Migrar usuário (Cria um novo usuário/schema Oracle e migra tudo que pertence ao usuário)', variable=self.migration_mode, onvalue=2, command=lambda:listbox.config(state='disabled')).pack(padx=20, pady=20)
        mmref.invoke()
        ttk.Button(lower_frame, text='Cancelar', command=self.close).pack(side='left', padx=20, pady=10)
        ttk.Button(lower_frame, text='Prosseguir', command=lambda:self.pass_schema_selection(listbox)).pack(side='right', padx=20, pady=10)

    # Função auxiliar para a seleção de schema
    def pass_schema_selection(self, listbox):
        try:
            schema = listbox.get(listbox.curselection()[0])
            if schema:
                self.user = schema
            elif self.migration_mode.get() != 2:
                return
            self.ora_users = self.execute_query('ora', "SELECT username FROM all_users WHERE oracle_maintained == 'N'")
            self.ora_users = [self.ora_users[i][0] for i in range(len(self.ora_users))]
            # Migração de usuário/schema
            if self.migration_mode.get() > 0 and not self.ora_dba:
                    self.message_window('Para migrar um schema ou usuário, por favor logue como um usuário DBA Oracle!')
                    return
            elif self.migration_mode.get() == 1:
                self.schema_migration_window(schema)
            elif self.migration_mode.get() == 2:
                self.user_migration_window(self.pg_user)
           # Migração de objetos
            else:
                self.draw_table_selection_window(schema)
        except Exception as e:
            print('Error: ' + str(e))

    def user_migration_window(self, user):
        # Coleta todos os objetos pertencentes ao usuário
        query = f"SELECT nsp.nspname AS object_schema, \
                cls.relname AS object_name, \
                rol.rolname AS owner, \
                CASE cls.relkind \
                    WHEN 'r' then 'TABLE' \
                    WHEN 'v' then 'VIEW' \
                    ELSE cls.relkind::text \
                END AS object_type \
                FROM pg_class cls \
                JOIN pg_roles rol ON rol.oid = cls.relowner \
                JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace \
                WHERE nsp.nspname NOT IN ('information_schema', 'pg_catalog') \
                AND nsp.nspname NOT LIKE 'pg_%' \
                AND rol.rolname = '{user}' \
                AND cls.relkind IN ('r', 'v') \
                UNION \
                SELECT ns.nspname, pc.proname, rl.rolname, \
                CASE pc.prokind \
                    WHEN 'f' THEN 'FUNCTION' \
                    WHEN 'p' THEN 'PROCEDURE' \
                    ELSE pc.prokind::text END \
                FROM pg_proc pc \
                JOIN pg_namespace AS nsp ON pc.pronamespace = nsp.oid \
                JOIN pg_roles AS rol ON pc.proowner = rol.oid \
                WHERE nsp.nspname NOT LIKE 'pg_%' AND nsp.nspname != 'information_schema' AND rol.rolname = '{user}'"
        self.objects = self.execute_query('pg', query)
        for object in self.objects:
            if object[3] == 'TABLE':
                # Coleta triggers associados as tabelas
                query = f"SELECT tg.tgname FROM pg_trigger tg \
                        JOIN pg_class AS cls ON tg.tgrelid = cls.oid \
                        JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace \
                        WHERE cls.relname = '{object[1]}' \
                        AND ns.nspname = '{object[0]}'\
                        AND tg.tgisinternal = 'false'"
                obj_trig = self.execute_query('pg', query)
                obj_trig = [obj_trig[i][0] for i in range(len(obj_trig))]
                object['trigger'] = {}
                for trig in obj_trig:
                    # Coleta função do trigger
                    query = f"SELECT pc.proname FROM pg_trigger tg JOIN pg_proc as pc ON tg.tgfoid = pc.oid WHERE tg.tgname = '{trig}'"
                    func = self.execute_query('pg', query)
                    object['trigger'][trig] = func[0][0]
        window = tk.Toplevel(self)
        ttk.Label(window, text=f'Objetos pertencentes ao usuário {user}:')
        box_frame = ttk.Frame(window)
        box_frame.pack(padx=10, pady=10)
        scrollBar = tk.Scrollbar(box_frame, orient='vertical')
        scrollBar.pack(side='right', fill='y')
        textBox = tk.Text(box_frame, yscrollcommand=scrollBar.set, state='normal', height=10)
        textBox.pack(pady=10)
        for object in self.objects:
            textBox.insert(tk.END, f"{object['object_schema']}.{object['object_name']}   {object['object_type']}")
        textBox.config(state='disabled')
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=20)
        ttk.Button(window, text='Prosseguir', command=lambda:[self.new_user_window(user), window.destroy()]).pack(side='right', padx=20, pady=20)

    def migrate_user(self, user, delete=False, password='', dba=False):
        try:
            cur = self.ora_conn.cursor()
            if delete:
                cur.execute(f'DROP USER {user} CASCADE')
            if user.upper not in self.ora_users:
                cur.execute(f'CREATE USER {user} IDENTIFIED BY {password}')
            if dba:
                cur.execute(f'GRANT DBA TO {user}')
            cur.close()
            self.ora_user = user
            self.ora_password = password
            self.ora_conn = jaydebeapi.connect(self.ora_driver, self.ora_url, [self.ora_user, self.ora_password], self.ora_jar)
            tables = []
            sources = []
            for object in self.objects:
                if object['object_type'] == 'TABLE':
                    tables.append([object['object_schema'], object['object_name']])
                    for trig_name, trig_func in object['trigger'].items():
                        sources.append([object['object_schema'], trig_name, 'TRIGGER'])
                        sources.append([object['schema_name'], trig_func, 'FUNCTION'])
                else:
                    sources.append([object['object_schema'], object['object_name'], object['object_type']])
            self.etl_initialization(self.ora_user, tables, sources)
        except Exception as e:
            print('Error ' + str(e))

    # Função auxiliar para decidir como migrar um schema
    def schema_migration_window(self, schema):
        window = tk.Toplevel(self)
        ttk.Label(window, text=f'Como prosseguir com a migração do schema {schema}?').pack(padx=20, pady=10)
        new_user = tk.BooleanVar()
        cbref = ttk.Checkbutton(window, text='Migrar objetos para schema conectado', variable=new_user, onvalue=False)
        cbref.pack(padx=20, pady=10)
        cbref.invoke()
        ttk.Checkbutton(window, text='Criar novo usuário/schema Oracle', variable=new_user, onvalue=True).pack(padx=20, pady=10)
        ttk.Button(window, text='Prosseguir', command=lambda:[self.pass_schema_migration(schema, new_user.get()), window.destroy()]).pack(side='right', padx=20, pady=10)
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)

    def pass_schema_migration(self, schema, new_user):
        if new_user:
            self.check_if_user_exists(schema)
        else:
            self.migrate_schema(schema)

    def new_user_window(self, user, delete):
        window = tk.Toplevel(self)
        ttk.Label(window, text=f'Digite uma senha para o novo usuário {user}:').pack(padx=20, pady=10)
        password_entry = ttk.Entry(window, width=30)
        password_entry.pack(padx=20, pady=10)
        dba_user = tk.BooleanVar()
        ttk.Checkbutton(window, text='Criar novo usuário como DBA', variable=dba_user, onvalue=True, offvalue=False).pack(padx=20, pady=10)
        ttk.Button(window, text='Prosseguir', command=lambda:[self.migrate_user(user, delete, password_entry.get(), dba_user.get()), window.destroy()]).pack(side='right', padx=20, pady=20)
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=20)

    # Pergunta o que fazer quando usuário já existe
    def check_if_user_exists(self, user):
        if user.upper() in self.ora_users:
            window = tk.Toplevel(self)
            ttk.Label(window, text=f'Usuário/Schema {user} já existe na base de dados!\nQual ação deseja tomar?').pack(padx=10, pady=10)
            ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
            if self.migration_mode.get() == 1:
                ttk.Button(window, text='Deletar e prosseguir', command=lambda:[self.new_user_window(user, True), window.destroy()]).pack(side='left', padx=20, pady=10)
                # TO DO
                ttk.Button(window, text='Manter e prosseguir', command=lambda:[self.migrate_schema(user, False), window.destroy()]).pack(side='left', padx=20, pady=10)
            elif self.migration_mode.get() == 2:
                ttk.Button(window, text='Deletar e prosseguir', command=lambda:[self.new_user_window(user, True), window.destroy()]).pack(side='left', padx=20, pady=10)
                ttk.Button(window, text='Manter e prosseguir', command=lambda:[self.new_user_window(user, False), window.destroy()]).pack(side='left', padx=20, pady=10)
        elif self.migration_mode.get() == 1:
            self.new_user_window(user, True)
        elif self.migration_mode.get() == 2:
            self.new_user_window(user, False)
        
    # Função para coletar configurações e migrar o schema
    def migrate_schema(self, schema, delete=False, new_user=False, new_user_pass='', new_dba_user=False):
        cur = self.ora_conn.cursor()
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
        tables = self.execute_query('pg', query)
        tables = [[schema, [tables[i][0]]] for i in range(len(tables))]
        sources = []
        # Adiciona procedimentos/funções do schema
        query = f"SELECT proname, prokind FROM pg_proc pc JOIN pg_namespace AS ns ON pc.pronamespace = ns.oid \
                WHERE ns.nspname = '{schema}'"
        sources_aux = self.execute_query('pg', query)
        for i in range(len(sources_aux)):
            if sources_aux[i][1] == 'f':
                sources.append([schema, sources_aux[i][0], 'FUNCTION'])
            elif sources_aux[i][1] == 'p':
                sources.append([schema, sources_aux[i][0], 'PROCEDURE'])
        # Adicions views do schema
        query = f"SELECT viewname FROM pg_views WHERE schemaname = '{schema}'"
        sources_aux = self.execute_query('pg', query)
        for source in sources_aux:
            sources.append([schema, source[0], 'VIEW'])
        # Adiciona trigger relacionados as tabelas do schema
        query = f"SELECT tgname FROM pg_catalog.pg_trigger tg JOIN pg_class AS cls \
                ON tg.tgrelid = cls.oid JOIN information_schema.tables AS tbl \
                ON cls.relname = tbl.table_name WHERE tbl.table_schema = '{schema}' \
                AND tg.tgisinternal = 'false'"
        sources_aux = self.execute_query('pg', query)
        for source in sources_aux:
            sources.append([schema, source[0], 'TRIGGER'])
            # Adiciona funções de trigger
            query = f"SELECT pc.proname FROM pg_trigger tg JOIN pg_proc as pc ON tg.tgfoid = pc.oid WHERE tg.tgname = '{source}'"
            func = self.execute_query('pg', query)
            sources.append([func[0][0], 'FUNCTION'])
        if len(tables) > 0 or len(sources) > 0:
            if delete:
                cur.execute(f'DROP USER {schema} CASCADE')
            if new_user:
                cur.execute(f'CREATE USER {schema} IDENTIFIED BY {new_user_pass}')
                self.ora_user = schema
                self.ora_password = new_user_pass
                cur.close()
                self.ora_conn = jaydebeapi.connect(self.ora_driver, self.ora_url, [self.ora_user, self.ora_password], self.ora_jar)
            if new_dba_user:
                cur.execute(f"GRANT DBA TO {schema}")
            self.etl_initialization(schema, tables, sources)
        else:
            return self.message_window(f'Schema {schema} não possui objetos!')

    # Procura dependencias entre os objetos e faz as preparações iniciais para começar o processo etl
    def etl_initialization(self, schema, tables, sources):
        if len(tables) == 0 and len(sources) == 0:
            window = tk.Toplevel(self)
            ttk.Label(window, text='Nenhuma tabela ou procedimento selecionado\nRetornando ao menu inicial').pack(padx=10, pady=10)
            ttk.Button(window, text='Ok', command=window.destroy).pack(pady=10)
            self.wait_window(window)
            return self.draw_schema_selection_window()
        if type(sources) != list:
            selected_sources = []
            for i in sources.curselection():
                split_src = sources.get(i).split()
                selected_sources.append([schema, split_src[0], split_src[1]])
            sources = selected_sources
        dep_tables = set()
        # Detecta dependencias entre tabelas
        for table in tables:
            query = f"SELECT table_schema, table_name FROM information_schema.table_constraints WHERE constraint_name IN \
                    (SELECT cls.relname FROM pg_constraint con JOIN pg_class cls ON con.conIndid = cls.oid WHERE con.conname IN \
                    (SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = '{table[0]}' \
                    AND table_name = '{table[1]}' AND constraint_type = 'FOREIGN KEY'))"
            r_tables = self.execute_query('pg', query)
            r_tables = [[r_tables[i][1], r_tables[i][0]] for i in range(len(r_tables))]
            for dep in r_tables:
                if dep[0] not in tables:
                    dep_tables.add(dep)
        dep_sources = set()
        if sources:
            # Detecta dependencias entre procedimentos
            for src in sources:
                if src[2] != 'TRIGGER':
                    # Postgresql não compila procedimentos, logo, não guarda dependencias na memória, apenas checando a tempo de execução
                    # Entretanto, é possível coletar as dependencias com um pouco de força bruta
                    query = f"SELECT schema, name, type FROM \
                            (SELECT table_schema as schema, table_name as name, 'TABLE' as type from information_schema.tables \
                            UNION SELECT ns.nspname, proname, CASE WHEN proc.prokind = 'f' THEN 'FUNCTION' ELSE 'PROCEDURE' END \
                            FROM pg_proc proc JOIN pg_namespace ns ON proc.pronamespace = ns.oid \
                            WHERE proc.proname != '{src[1]}' AND ns.nspname not like 'pg_%' AND ns.nspname != 'information_schema' \
                            UNION SELECT schemaname, viewname, 'VIEW' from pg_views where viewname != '{src[1]}') as names \
                            JOIN \
                            (SELECT pg_get_functiondef(p.oid) AS func_def \
                            FROM pg_proc p \
                            JOIN pg_namespace n ON n.oid = p.pronamespace \
                            WHERE n.nspname = '{src[0]}' and p.proname = '{src[1]}' \
                            UNION SELECT definition FROM pg_views \
                            WHERE schemaname = '{src[0]}' AND viewname = '{src[1]}') as body \
                            ON position(CASE WHEN names.schema != 'public' THEN COALESCE(names.schema || '.' || names.name, '') \
                            ELSE names.name END in body.func_def) > 0"
                    dependencies = self.execute_query('pg', query)
                    dependencies = [[dependencies[i][0], dependencies[i][1], dependencies[i][2]] for i in range(len(dependencies))]
                    # Detecta se procedimento depende de uma tabela ou outro procedimento
                    for dep in dependencies:
                        if dep[2] == 'TABLE':
                            if [dep[0], dep[1]] not in tables:
                                dep_tables.add([dep[0], dep[1]])
                        elif dep not in sources:
                            dep_sources.add(tuple(dep))
                else:
                    # Checa se as tabelas relacionadas aos triggers estão inclusas
                    query = f"SELECT ns.nspname, cls.relname FROM pg_trigger tg JOIN pg_class AS cls ON tg.tgrelid = cls.oid \
                            JOIN pg_namespace AS ns ON cls.relnamespace = ns.oid WHERE tg.tgname = '{src[1]}'"
                    trig_dep = self.execute_query('pg', query)
                    for tab in trig_dep:
                        if tab not in tables:
                            dep_tables.add(tab)
                    # Checa se existe alguma sequencia referenciada pela função do trigger
                    query = f"SELECT ns.nspname, cls.relname FROM pg_class cls JOIN pg_namespace AS ns \
                            ON cls.relnamespace = ns.oid WHERE cls.relname IN \
                            (SELECT relname FROM \
                            (SELECT cls.relname FROM pg_sequence seq JOIN pg_class AS cls \
                            ON seq.seqrelid = cls.oid JOIN pg_namespace AS ns \
                            ON cls.relnamespace = ns.oid WHERE ns.nspname = '{src[0]}') AS names \
                            JOIN \
                            (SELECT pg_get_functiondef(proc.oid) AS func_def FROM pg_proc proc \
                            JOIN pg_namespace AS ns ON ns.oid = proc.pronamespace \
                            WHERE ns.nspname = '{src[0]}' AND proc.proname IN ( \
                            SELECT pr.proname FROM pg_trigger tgr JOIN pg_proc AS pr \
                            ON tgr.tgfoid = pr.oid WHERE tgr.tgname = '{src[1]}' \
                            )) AS body \
                            ON position(names.relname IN body.func_def) > 0)"
                    seq_dep = self.execute_query('pg', query)
                    seq_dep = [[seq_dep[i][0], seq_dep[i][1]] for i in range(len(seq_dep))]
                    for seq in seq_dep:
                        sources.append([seq[0], seq[1], 'SEQUENCE'])
            dep_sources = list(dep_sources)
            dep_tables = list(dep_tables)
        if len(dep_tables) > 0 or len(dep_sources) > 0:
            self.draw_dep_window(dep_tables, tables, dep_sources, sources, schema)
        else:
            self.begin_etl_session(schema, tables, sources)

    # Lista dependencias entre os objetos escolhidos e pede permissão para incluí-los à migração
    def draw_dep_window(self, dep_tables, sel_tables, dep_sources, sel_sources, schema):
        window = tk.Toplevel(self)
        ttk.Label(window, text='Algumas das tabelas/procedimentos selecionados dependem das seguintes tabelas/procedimentos\nPara prosseguir elas tambem serão migradas').pack(padx=10, pady=10)
        box_frame = ttk.Frame(window)
        box_frame.pack(padx=10, pady=10)
        scrollBar = tk.Scrollbar(box_frame, orient='vertical')
        scrollBar.pack(side='right', fill='y')
        textBox = tk.Text(box_frame, yscrollcommand=scrollBar.set, state='normal', height=10)
        textBox.pack(pady=10)
        for table in dep_tables:
            if table[0] == 'public':
                textBox.insert(tk.END, f'{table[1]}   TABLE\n')
            else:
                textBox.insert(tk.END, f'{table[0]}.{table[1]}   TABLE\n')
            sel_tables.append(table)
        for src in dep_sources:
            if src[0] == 'public':
                textBox.insert(tk.END, f'{src[1]}   {src[2]}\n')
            else:
                textBox.insert(tk.END, f'{src[0]}.{src[1]}   {src[2]}\n')
            sel_sources.append(src)
        textBox.config(state='disabled')
        lower_frame = ttk.Frame(window)
        lower_frame.pack()
        ttk.Button(lower_frame, text='Concordar', command=lambda: [self.etl_initialization(schema, sel_tables, sel_sources), window.destroy()]).pack(side='right', padx=20, pady=10)
        ttk.Button(lower_frame, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)

    # Inicializa a sessão de etl e começa a migrar
    @threaded
    def begin_etl_session(self, tables, sources):
        pg_conf = {'host': self.pg_host, 'port': self.pg_port,'user': self.pg_user, 'password': self.pg_password, 'database': self.pg_database}
        ora_conf = {'host': self.ora_host, 'port': self.ora_port, 'user': self.ora_user, 'password': self.ora_password, 'service': self.ora_service}
        if not self.full_backup:
            self.write2log(f'Criando backup do schema {self.ora_user}')
            self.create_backup(self.ora_user)
        self.write2log(f'Iniciando sessão {self.__hash__()} de Postgres {self.pg_url} para Oracle {self.ora_url}')
        etl_session = Postgres2OracleETL(self.master, pg_conf, ora_conf, self.user, tables, sources, self.etl, self.pg_jar, self.ora_jar, self.pg_schema)
        self.write2log(f"migrando tabelas {tables} e sources {sources} do Oracle schema '{self.user}' para Postgres database {self.pg_database} schema '{self.pg_schema}'!")
        self.active_sessions.append(etl_session)
        # Começa a migração
        etl_session.start_etl()
        self.wait4ETL(etl_session)
        self.draw_schema_selection_window()

    def wait4ETL(self, session):
        while session._state == 'idle' or session._state == 'executing':
            sleep(1)
        if session._state == 'success':
            self.write2log('Migração concluída com sucesso!')
        elif session._state == 'failed':
            if not self.full_backup:
                self.write2log('Migração falhou! Retornando base de dados para estado anterior...')
                self.restore_from_backup(self.pg_schema)
                self.write2log('Base de dados retornada ao último estado estável')
            print(session.error_message)
        self.remove_backup(self.pg_schema)
        self.active_sessions.remove(session)
        self.write2log(f'Terminada sessão de ETL {self.etl.__hash__()}')
        del session