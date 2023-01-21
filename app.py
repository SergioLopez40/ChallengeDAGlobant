from flask import Flask, request
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///hiredEmployers.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class jobs(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f'<jobs {self.job}>'

class departments(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f'<departments {self.department}>'

class hired_employees(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    datetime = db.Column(db.String(100), nullable=False)
    department_id = db.Column(db.Integer, nullable=False)
    job_id = db.Column(db.Integer, nullable=False)

    def __repr__(self):
        return f'<hired_employees {self.hired_employee}>'

@app.route('/', methods=['GET'])
def hello():
    db.create_all()
    return 'Succesful'

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000, debug=True)