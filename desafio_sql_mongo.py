from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import func
from sqlalchemy import DECIMAL


Base = declarative_base()


class Client(Base):
    __tablename__ = "client_account"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    cpf = Column(String(14), unique=True, nullable=False)
    endereco = Column(String(50), unique=True, nullable=False)
    count = relationship("Count", back_populates="client", cascade="all, delete-orphan")

    def __repr__(self):
        return f"Client(Id= {self.id}, Name= {self.name}, CPF = {self.cpf},Address = {self.endereco})"


class Count(Base):
    __tablename__ = "count"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tipo = Column(String, default="Current Account", nullable=False)
    agency = Column(String, nullable=False)
    number = Column(Integer, unique=True)
    id_client = Column(Integer, ForeignKey("client_account.id"), nullable=False)
    balance = Column(DECIMAL(), nullable=False)
    client = relationship("Client", back_populates="count")

    def __repr__(self):
        return f"\nCount(Id= {self.id}, Type= {self.tipo}, Agency= {self.agency}," \
               f"Account Number={self.number}, Balance= {self.balance})"


print(Client.__tablename__)
print(Count.__tablename__)

engine = create_engine("sqlite://")
Base.metadata.create_all(engine)
inspector_engine = inspect(engine)
print(inspector_engine.has_table("count"))
print(inspector_engine.has_table("client_account"))
print(inspector_engine.get_table_names())
print(inspector_engine.default_schema_name)

with Session(engine) as session:
    pedro = Client(name="Pedro Souza", cpf="111.111.111-11", endereco="Rua j, 15",
                   count=[Count(agency="015-9", number="1111", balance="500.00")])

    luely = Client(name="Luely Matos", cpf="222.222.222-22", endereco="Rua A, 35",
                   count=[Count(agency="015-9", number="2222", balance="4500.00")])

    arthur = Client(name="Arthur Matos", cpf= "333.333.333-33", endereco="Rua C, 35",
                   count=[Count(agency="015-9", number="3333", balance="150.00")])

    session.add_all([pedro, luely, arthur])

    session.commit()


stmt = select(Client).where(Client.name.in_(['Pedro Souza', 'Luely Matos', 'Arthur Matos']))
for client in session.scalars(stmt):
    print("\nCliente Cadastrado: ")
    print(client)

stmt_count = select(Count).where(Count.id_client.in_([3]))
for count in session.scalars(stmt_count):
    print(count)




stmt_order = select(Client).order_by(Client.cpf.desc())
print("\nClients in decrescent order:")
for order in session.scalars(stmt_order):
    print(order)




stmt_join = select(Client.name, Count.number).join_from(Client,Count)
print('\n Recovering datas clients')
for order in session.scalars(stmt_join):
    print(order)




connection = engine.connect()
results = connection.execute(stmt_join).fetchall()
print("Executing from connection stmt")
for result in results:
    print(result)

stmt_count = select(func.count('*')).select_from(Client)
print("\n Counted Instaces;")
for order in session.scalars(stmt_count):
    print(order)

"""Mongo Atlas Integration"""
import datetime
import pprint
import pymongo
from pymongo import MongoClient
Uri ="mongodb+srv://pedrosouza:88599724@pedrocluster.yot0umw.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(Uri)




db = client.Matriz
collection = db.Bank
#print(db.Bank)


client = [
    {"Cliente": "Pedro Souza",
    "CPF": "111.111.111-11",
    "Address": "Rua J , 15",
    "Agency": "015-9",
    "Number Account": "1111",
    "Type": "Current Account",
    "Balance": "500,00",
    "Date": datetime.datetime.utcnow(),
    "Tags":["Pedro", "Souza", "111"]},


    {"Cliente": "Luely Matos",
    "CPF": "222.222.222-22",
    "Address": "Rua A, 35",
    "Agency": "015-9",
    "Number Account": "2222",
    "Type": "Current Account",
    "Balance": "4500,00",
    "Date": datetime.datetime.utcnow(),
    "Tags": ["Luely", "Matos", "222"]},


    {"Cliente": "Arthur Matos",
    "CPF": "333.333.333-33",
    "Address": "Rua C, 35",
    "Agency": "015-9",
    "Number Account": "3333",
    "Type": "Current Account",
    "Balance": "150,00",
    "Date": datetime.datetime.utcnow(),
    "Tags": ["Arthur", "Matos", "333"]}]

clients = db.clients
#client_id = clients.insert_many(client)
#print(client_id.inserted_ids)

print("\nRecovering docs in date order")
for client in (clients.find({}).sort("date")):
    pprint.pprint(client)

result = db.clients.create_index([('Number Account', pymongo.ASCENDING)],
                                  unique=True)
print(sorted(list(db.clients.index_information())))

