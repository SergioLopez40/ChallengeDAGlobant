import logging.config
import pandas as pd
import os
from flask import Flask, request, render_template, url_for, redirect, flash, abort
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from werkzeug.utils import secure_filename

#Definicion de la configuracion para los logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log_file.log"),
        logging.StreamHandler()
    ]
)
#Creacion logger
logger = logging.getLogger(__name__)

UPLOAD_FOLDER = './source/'
ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)

#Configuracion de nuestra base de datos sqlite
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///hiredEmployers.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#Configuracion folders de carga de archivos
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

db = SQLAlchemy(app)

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
    pd_dep = na_free.values.tolist()
    for x in pd_dep:
        Department = department(id = x[0], department= x[1])
        db.session.add(Department)
        logger.info('department '+ str(Department.id) + ' ' + Department.department + ' cargado exitosamente')
    db.session.commit()
    na_dep = only_na.values.tolist()
    for x in na_dep:
        Department = department(id = x[0], department= x[1])
        logger.error('department '+ str(Department.id) + ' no se ha cargado, debido a que contiene un campo nulo')
    
    #Carga de archivo hired_employees.csv
    hired_employees = pd.read_csv('source/hired_employees.csv', header=None,names=['id','name','datetime','departmen_id','job_id'])
    na_free = hired_employees.dropna()
    only_na = hired_employees[~hired_employees.index.isin(na_free.index)]
    pd_he = na_free.values.tolist()
    for x in pd_he:
        employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
        db.session.add(employee)
        logger.info('hired_employee '+ str(employee.id) + ' ' + employee.name + ' cargado exitosamente')
    db.session.commit()
    na_he = only_na.values.tolist()
    for x in na_he:
        employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
        logger.error('hired_employee '+ str(employee.id) + ' no se ha cargado, debido a que contiene un campo nulo')

def upload_data(table, filepath):
    if(table == 'job'):
        jobs = pd.read_csv(filepath, header=None,names=['id','job'])
        na_free = jobs.dropna()
        only_na = jobs[~jobs.index.isin(na_free.index)]
        if (len(na_free.columns) != 2):
            logger.error('No se ha cargado ningun registro del archivo debido a que no coinciden las cantidad de columnas del archivo a la definicion dela tabla job')
        elif (len(na_free.columns) == 2):
            pd_job = na_free.values.tolist()
            for x in pd_job:
                Job = job(id = x[0], job= x[1])
                db.session.add(Job)
                logger.info('job '+ str(Job.id) + ' ' + str(Job.job) + ' cargado exitosamente')
            db.session.commit()
            na_job = only_na.values.tolist()
            for x in na_job:
                Job = job(id = x[0], job= x[1])
                logger.error('job '+ str(Job.id) + ' no se ha cargado, debido a que contiene un campo nulo')  
    elif(table == 'department'):
        departments = pd.read_csv(filepath, header=None,names=['id','department'])
        na_free = departments.dropna()
        only_na = departments[~departments.index.isin(na_free.index)]
        if (len(na_free.columns) != 2):
            print("es distinto de 2")
            logger.error('No se ha cargado ningun registro del archivo debido a que no coinciden las cantidad de columnas del archivo a la definicion dela tabla department')
        elif (len(na_free.columns) == 2):
            print("es igual a 2")
            pd_dep = na_free.values.tolist()
            for x in pd_dep:
                Department = department(id = x[0], department= x[1])
                db.session.add(Department)
                logger.info('department '+ str(Department.id) + ' ' + str(Department.department) + ' cargado exitosamente')
            db.session.commit()
            na_dep = only_na.values.tolist()
            for x in na_dep:
                Department = department(id = x[0], department= x[1])
                logger.error('department '+ str(Department.id) + ' no se ha cargado, debido a que contiene un campo nulo')
    elif(table == 'hired_employee'):
        hired_employees = pd.read_csv(filepath, header=None,names=['id','name','datetime','departmen_id','job_id'])
        na_free = hired_employees.dropna()
        only_na = hired_employees[~hired_employees.index.isin(na_free.index)]
        if (len(na_free.columns) != 5):
            logger.error('No se ha cargado ningun registro del archivo debido a que no coinciden las cantidad de columnas del archivo a la definicion dela tabla department')
        elif (len(na_free.columns) == 5):
            pd_he = na_free.values.tolist()
            for x in pd_he:
                employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
                db.session.add(employee)
                logger.info('hired_employee '+ str(employee.id) + ' ' + str(employee.name) + ' cargado exitosamente')
            db.session.commit()
            na_he = only_na.values.tolist()
            for x in na_he:
                employee = hired_employee(id = x[0], name= x[1], datetime= x[2],department_id= x[3], job_id=x[4])
                logger.error('hired_employee '+ str(employee.id) + ' no se ha cargado, debido a que contiene un campo nulo')

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload')
def index():
     # Set The upload HTML template '\templates\index.html'
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
       file = request.files['file']
       table = request.form.get('table')
       # empty file without a filename.
       if file.filename == '':
            abort(400)
       if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            upload_data(table, filepath)
            return render_template('cargaOk.html')
    return render_template('index.html')

@app.route('/', methods=['GET'])
def hello():
    db.drop_all()
    db.create_all()
    csv_historicData()
    return ''

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000, debug=True)