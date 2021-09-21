from fastapi import FastAPI, HTTPException
from typing import Text, Optional
from datetime import datetime
from uuid import uuid4 as uuid

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

objectFlask = Flask(__name__)
objectFlask.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:bigdata@localhost/xaldigital'
objectFlask.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

#Inicializaicon del objeto db de sqlalchemy
db = SQLAlchemy(objectFlask)

class Sample(db.Model):
    first_name = db.Column(db.String(), primary_key= True)    
    last_name = db.Column(db.String())
    company_name = db.Column(db.String())
    address = db.Column(db.String())
    city = db.Column(db.String())
    state  = db.Column(db.String())
    zip = db.Column(db.String())
    phone1 = db.Column(db.String())
    phone2 = db.Column(db.String())
    email = db.Column(db.String())
    department = db.String()

app1 = FastAPI()

# post model 
@app1.get("/samples")
def get_samples():
    samples_result = Sample.query.all()    
    return samples_result 

@app1.get("/")
def read_root():    
    return {"Hola": "Bienvenido a REST API"}