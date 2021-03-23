from sqlalchemy import create_engine, func
import settings
import datetime
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

engine = create_engine(settings.DB_URL, echo=False)

metadata = MetaData()
from sqlalchemy import Column, Integer, String
class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    data = Column(String)
    term = Column(String)
    page = Column(Integer)
    published = Column(DateTime)
    nyt_id = Column(String, unique=True)
    start_date = Column(Date)
    end_date = Column(Date)

    created = Column(DateTime, server_default=func.now())
    modified = Column(DateTime, server_default=func.now(), onupdate=func.current_timestamp())

    def __repr__(self):
        return "<Article(term='{0}', page='{1}', id='{2}', start_date='{3}', end_date='{4}')>".format(self.term, self.page, self.id, self.start_date, self.end_date)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
from collections import defaultdict
if __name__ == '__main__':
    session = Session()
    start_date = settings.START_DATE
    end_date = settings.END_DATE
    freq = defaultdict(lambda: defaultdict(int))
    for a in session.query(Article).filter_by(term='protest*').order_by(Article.created.desc()):
        freq[f'{a.start_date}-{a.end_date}'][a.page] += 1
    for d in freq.keys():
        for i in range(sorted(freq[d].keys())[-1]):
            if i not in freq[d] or freq[d][i] != 10:
                print(d, i)
