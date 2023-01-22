#Importacion de los recursos necesarios para la ejecucion
import logging.config
import pandas as pd
import os
from flask import Flask, request, render_template, url_for, redirect, flash, abort, json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
from sqlalchemy import create_engine, text
from werkzeug.utils import secure_filename

#Definicion de la configuracion para los logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/log_file.log"),
        logging.StreamHandler()
    ]
)
#Creacion logger
logger = logging.getLogger(__name__)

#Extensiones permitidas para hacer upload
UPLOAD_FOLDER = './source/'
ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)

#Configuracion de nuestra base de datos sqlite, pool size/timeout
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///hiredEmployers.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_POOL_SIZE'] = None
app.config['SQLALCHEMY_POOL_TIMEOUT'] = None
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

db = SQLAlchemy(app)

#--------------------------------------------------------------------------------------------------------------
###Definicion de los modelos de la base datos
#Definicion Modelo para tabla Job
class job(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job = db.Column(db.String(100), nullable=False)

    def __init__(self, id, job):
        self.id = id
        self.job = job

    def __repr__(self):
        return f'<{self.id} job {self.job}>'

#Definicion Modelo para tabla department
class department(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    department = db.Column(db.String(100), nullable=False)

    def __init__(self, id, department):
        self.id = id
        self.department = department

    def __repr__(self):
        return f'<department {self.department}>'

#Definicion Modelo para tabla hired_employee
class hired_employee(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    datetime = db.Column(db.String(100), nullable=False)
    department_id = db.Column(db.Integer, nullable=False)
    job_id = db.Column(db.Integer, nullable=False)

    def __init__(self, id, name, datetime, department_id, job_id):
        self.id = id
        self.name = name
        self.datetime = datetime
        self.department_id = department_id
        self.job_id = job_id

    def __repr__(self):
        return f'<employee {self.name}>'

#--------------------------------------------------------------------------------------------------------------
#Funcion para cargar los datos historicos a la base de datos
def csv_historicData():
    #Carga de archivo jobs.csv
    #Lectura del archivo.csv con pandas, se crea un data frame para los registros que respetan las data rules(all fields required) y uno para los que no por medio de dropna()
    jobs = pd.read_csv('source/jobs.csv', header=None,names=['id','job'])
    na_free = jobs.dropna()
    only_na = jobs[~jobs.index.isin(na_free.index)]
    #Creamos la lista a insertar, iteramos instanciando y añadiendo a la sesion, se confirman los cambios en la base de datos y se registra en el log
    pd_job = na_free.values.tolist()
    for x in pd_job:
        Job = job(id = x[0], job= x[1])
        db.session.add(Job)
        logger.info('job '+ str(Job.id) + ' ' + Job.job + ' cargado exitosamente')
    db.session.commit()
    #Creamos la lista a registrar en el log de los registros que no se cargan debido a campos nulos
    na_job = only_na.values.tolist()
    for x in na_job:
        Job = job(id = x[0], job= x[1])
        logger.error('job '+ str(Job.id) + ' no se ha cargado, debido a que contiene un campo nulo')

    #Carga de archivo departments.csv
    departments = pd.read_csv('source/departments.csv', header=None,names=['id','department'])
    na_free = departments.dropna()
    only_na = departments[~departments.index.isin(na_free.index)]
    #Creamos la lista a insertar, iteramos instanciando y añadiendo a la sesion, se confirman los cambios en la base de datos y se registra en el log
    pd_dep = na_free.values.tolist()
    for x in pd_dep:
        Department = department(id = x[0], department= x[1])
        db.session.add(Department)
        logger.info('department '+ str(Department.id) + ' ' + Department.department + ' cargado exitosamente')
    db.session.commit()
    #Creamos la lista a registrar en el log de los registros que no se cargan debido a campos nulos
    na_dep = only_na.values.tolist()  
    for x in na_dep:
        Department = department(id = x[0], department= x[1])
        logger.error('department '+ str(Department.id) + ' no se ha cargado, debido a que contiene un campo nulo')
    
    #Carga de archivo hired_employees.csv
    hired_employees = pd.read_csv('source/hired_employees.csv', header=None,names=['id','name','datetime','departmen_id','job_id'])
    na_free = hired_employees.dropna()
    only_na = hired_employees[~hired_employees.index.isin(na_free.index)]
    #Creamos la lista a insertar, iteramos instanciando y añadiendo a la sesion, se confirman los cambios en la base de datos y se registra en el log
    pd_he = na_free.values.tolist()
    for x in pd_he:
        employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
        db.session.add(employee)
        logger.info('hired_employee '+ str(employee.id) + ' ' + employee.name + ' cargado exitosamente')
    db.session.commit()
    #Creamos la lista a registrar en el log de los registros que no se cargan debido a campos nulos
    na_he = only_na.values.tolist()
    for x in na_he:
        employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
        logger.error('hired_employee '+ str(employee.id) + ' no se ha cargado, debido a que contiene un campo nulo')

#--------------------------------------------------------------------------------------------------------------
##Funcion para cargar nuevos datos por medio de un formulario que recibe archivos csv
def upload_data(table, filepath):
    #Validacion de a que tabla se desean cargar la informacion del archivo a cargar
    if(table == 'job'):
        jobs = pd.read_csv(filepath, header=None,names=['id','job'])
        na_free = jobs.dropna()
        only_na = jobs[~jobs.index.isin(na_free.index)]
        #Luego de crear el dataset validamos que la informacion del archivo corresponda a las reglas de la tabla.
        if (len(na_free.columns) != 2):
            logger.error('No se ha cargado ningun registro del archivo debido a que no coinciden las cantidad de columnas del archivo a la definicion dela tabla job')
        elif (len(na_free.columns) == 2):
            #Generacion del la lista a insertar, creacion del objeto job, la sesion y cargue en la base de datos.
            pd_job = na_free.values.tolist()
            for x in pd_job:
                Job = job(id = x[0], job= x[1])
                db.session.add(Job)
                logger.info('job '+ str(Job.id) + ' ' + str(Job.job) + ' cargado exitosamente')
            db.session.commit()
            #Si habia registros con campos nulos se registran en el log como no cargados
            na_job = only_na.values.tolist()
            for x in na_job:
                Job = job(id = x[0], job= x[1])
                logger.error('job '+ str(Job.id) + ' no se ha cargado, debido a que contiene un campo nulo')  
    #Validacion de a que tabla se desean cargar la informacion del archivo a cargar
    elif(table == 'department'):
        departments = pd.read_csv(filepath, header=None,names=['id','department'])
        na_free = departments.dropna()
        only_na = departments[~departments.index.isin(na_free.index)]
        #Luego de crear el dataset validamos que la informacion del archivo corresponda a las reglas de la tabla.
        if (len(na_free.columns) != 2):
            logger.error('No se ha cargado ningun registro del archivo debido a que no coinciden las cantidad de columnas del archivo a la definicion dela tabla department')
        elif (len(na_free.columns) == 2):
            #Generacion del la lista a insertar, creacion del objeto job, la sesion y cargue en la base de datos.
            pd_dep = na_free.values.tolist()
            for x in pd_dep:
                Department = department(id = x[0], department= x[1])
                db.session.add(Department)
                logger.info('department '+ str(Department.id) + ' ' + str(Department.department) + ' cargado exitosamente')
            db.session.commit()
            #Si habia registros con campos nulos se registran en el log como no cargados
            na_dep = only_na.values.tolist()
            for x in na_dep:
                Department = department(id = x[0], department= x[1])
                logger.error('department '+ str(Department.id) + ' no se ha cargado, debido a que contiene un campo nulo')
    #Validacion de a que tabla se desean cargar la informacion del archivo a cargar
    elif(table == 'hired_employee'):
        hired_employees = pd.read_csv(filepath, header=None,names=['id','name','datetime','departmen_id','job_id'])
        na_free = hired_employees.dropna()
        only_na = hired_employees[~hired_employees.index.isin(na_free.index)]
        #Luego de crear el dataset validamos que la informacion del archivo corresponda a las reglas de la tabla.
        if (len(na_free.columns) != 5):
            logger.error('No se ha cargado ningun registro del archivo debido a que no coinciden las cantidad de columnas del archivo a la definicion dela tabla department')
        elif (len(na_free.columns) == 5):
            #Generacion del la lista a insertar, creacion del objeto job, la sesion y cargue en la base de datos.
            pd_he = na_free.values.tolist()
            for x in pd_he:
                employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
                db.session.add(employee)
                logger.info('hired_employee '+ str(employee.id) + ' ' + str(employee.name) + ' cargado exitosamente')
            db.session.commit()
            #Si habia registros con campos nulos se registran en el log como no cargados
            na_he = only_na.values.tolist()
            for x in na_he:
                employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
                logger.error('hired_employee '+ str(employee.id) + ' no se ha cargado, debido a que contiene un campo nulo')

##Funcion para validar el formato del archivo .csv
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

#--------------------------------------------------------------------------------------------------------------
#Rutas del API
#Definicion ruta para el servicio de cargue de nuevos archivos csv.
@app.route('/upload', methods=['POST', 'GET'])
def upload_file():
    if request.method == 'POST':
       #Se recibe el archivo de la peticion
       file = request.files['file']
       #se recibe la tabla a la cual va a insertar
       table = request.form.get('table')
       #Validacion si archivo vacio o no seleccionado
       if file.filename == '':
            abort(400)
       #Validacion formato del archivo usando allowed_file function
       if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            #Se guarda el archivo en la ruta especifica
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            #Se realiza el cargue usando upload_data function con el archivo y la tabla como parametros
            upload_data(table, filepath)
            return render_template('cargaOk.html')
    else:
        return render_template('index.html')

#Definicion endpoint challenge #2 - metrica #1
@app.route('/employeesByQ', methods=['GET'])
def consultaSQL1():
    #Creamos el query utilizando la funcion text()
    sql_text0 = text("""
    select department, job 
        ,count(case when substr(he.datetime,6,2) in ('01','02','03') then he.id end) as Q1
        ,count(case when substr(he.datetime,6,2) in ('04','05','06') then he.id end) as Q2
        ,count(case when substr(he.datetime,6,2) in ('07','08','09') then he.id end) as Q3
        ,count(case when substr(he.datetime,6,2) in ('10','11','12') then he.id end) as Q4
    from
    hired_employee he 
    join job j on (he.job_id = j.id)
    join department d on (he.department_id = d.id)
    where substr(he.datetime,1,4) = '2021'
    group by job, department
    order by department, job;""") 
    #Creacion del motor y la conexion con la base de datos
    db_url = app.config['SQLALCHEMY_DATABASE_URI']
    engine = create_engine(db_url)
    conn = engine.connect()
    #Ejecutamos el query
    result = conn.execute(sql_text0, pool_size=app.config['SQLALCHEMY_POOL_SIZE'])
    #Recuperamos el resultado y cerramos la coneccion
    Resultfinal = result.fetchall()
    conn.close()
    #Creamos un dataframe con pandas, que utilizamos para guardar el resultado del query
    final = pd.DataFrame()
    for x in Resultfinal:
        final2 = pd.DataFrame(list(x)).T
        final = pd.concat([final,final2])
    #Renombramos las columnas del dataframe de acuerdo a la consulta
    final.columns = [' department ', 'job', ' Q1 ', ' Q2 ', ' Q3 ', ' Q4 ']
    #Creamos el template utilizando el dataframe
    final.to_html('./templates/employeesByQ.html', index=False, index_names=False, justify='justify-all')
    #Renderizamos el template con la informacion de la consulta en forma de tabla
    return render_template('employeesByQ.html')


#Definicion endpoint challenge #2 - metrica #1
@app.route('/employeesByDepartment', methods=['GET'])
def consultaSQL2():
    #Creamos los querys necesarios
    #0. Contar el total de los departments
    #1. Contar el total de los hired_employees en el año 2021
    #2. Traer el id, department y hired_employees de cada departamento que tiene una cantidad de empleados contratados mayor al promedio del año 2021 para todos los departamentos
    sql_text0 = text("select count(1) from department;") 
    sql_text1 = text("select count(1) from hired_employee where substr(datetime,1,4) = '2021';")
    sql_text2 = text("""
    select d.id, d.department, count(he.id) as hired 
    from hired_employee he 
    join department d on (he.department_id = d.id)
    where substr(he.datetime,1,4) = '2021'
    group by department
    having hired > :mean
    order by hired desc;""") 
    #Creacion del motor y la conexion con la base de datos
    db_url = app.config['SQLALCHEMY_DATABASE_URI']
    engine = create_engine(db_url)
    conn = engine.connect()
    #Ejecutamos el primer query y guardamos el resultado
    a = conn.execute(sql_text0, pool_size=app.config['SQLALCHEMY_POOL_SIZE'])
    Tot_departments = a.fetchone()[0]
    #Ejecutamos el segundo query y guardamos el resultado
    b = conn.execute(sql_text1, pool_size=app.config['SQLALCHEMY_POOL_SIZE'])
    Tot_hiredEmpolyees = b.fetchone()[0]
    #Calculamos el promedio de contratados del año 2021 para todos los departamentos
    mean = int(Tot_hiredEmpolyees/Tot_departments) 
    #Ejecutamos el tercer query
    result = conn.execute(sql_text2, pool_size=app.config['SQLALCHEMY_POOL_SIZE'], mean=mean)
    Resultfinal = result.fetchall()
    conn.close()
    #Creamos un dataframe con pandas, que utilizamos para guardar el resultado del query
    final = pd.DataFrame()
    for x in Resultfinal:
        final2 = pd.DataFrame(list(x)).T
        final = pd.concat([final,final2])
    #Renombramos las columnas del dataframe de acuerdo a la consulta
    final.columns = [' id ', 'department', ' hired ']
    #Creamos el template utilizando el dataframe
    final.to_html('./templates/employeesByDepartment.html', index=False, index_names=False, justify='justify-all')
    #Renderizamos el template con la informacion de la consulta en forma de tabla
    return render_template('employeesByDepartment.html')

@app.route('/', methods=['GET'])
def hello():
    db.drop_all()
    db.create_all()
    csv_historicData()
    return ''

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000, debug=True)