from sqlmodel import SQLModel, create_engine, Session
import os
import logfire

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:root@mysql:3306/pipeline")

engine = create_engine(DATABASE_URL)
logfire.instrument_sqlalchemy(engine=engine)

def get_session():
    with Session(engine) as session:
        yield session

def init_db():
    SQLModel.metadata.create_all(engine)
