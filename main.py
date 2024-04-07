import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import os
from datetime import datetime
import json

Base = declarative_base()
login = os.getenv('db_login')
pas = os.getenv('db_password')
db_name = 'hw4'

DSN = f"postgresql://{login}:{pas}@localhost:5432/{db_name}"
engine = sq.create_engine(DSN)

# сессия
Session = sessionmaker(bind=engine)
session = Session()

class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __repr__(self):
        return f'{self.name}'
    
class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.Text, nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)
    publishers = relationship("Publisher", backref="books")

    def __repr__(self):
        return f'{self.title}'

class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)
    def __repr__(self):
        return f'{self.name}'
    
class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    books = relationship("Book", backref="stock_books")
    shops = relationship("Shop", backref="stock_shops")
    count = sq.Column(sq.Integer, nullable=False)

class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Numeric(10, 2), nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False, default=datetime.now)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    stocks = relationship("Stock", backref="sale_stock")

    def __repr__(self):
        date = datetime.fromisoformat(str(self.date_sale)).strftime('%d-%m-%Y')
        return f'{round(self.price, 0)} | {date}'
    

def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

create_tables(engine)

with open('tests_data.json') as f:
    data = json.load(f)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

def query_publisher(pub):
    #result = session.query(Book, Shop, Sale).filter(Publisher.name == query).filter(Publisher.id == Book.id_publisher).filter(Book.id == Stock.id_book).filter(Stock.id_shop == Shop.id).filter(Stock.id == Sale.id_stock).all()
    result = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Publisher).join(Stock).join(Shop).join(Sale)
    if pub.isdigit():
        query = result.filter(Publisher.id == pub).all()
    else:
        query = result.filter(Publisher.name == pub).all()
    
    for r in query:
        print(f'{r[0]} | {r[1]} | {r[2]}')

pub = input('Введите имя издателя: ')
query_publisher(pub)
