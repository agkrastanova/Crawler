import sys
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager


engine = create_engine('sqlite:///mycrawler.db')
Session = sessionmaker(bind=engine)

Base = declarative_base()


class Urls(Base):
    __tablename__ = 'urls'
    url_id = Column(Integer, primary_key=True)
    url = Column(String)


Base.metadata.create_all(engine)


@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_if_is_not_in_db_and_add(new_url, next_url):
    with session_scope() as session:
        is_in_db = session.query(Urls).filter(Urls.url == next_url).first()
        if is_in_db is None:
            session.add(new_url)
            session.commit()


def main(url):

    given_url = url
    urls_list = [given_url]

    while len(urls_list):
        next_url = urls_list[0]
        urls_list.remove(urls_list[0])

        response = requests.get(next_url)

        new_url = Urls(url=next_url)

        check_if_is_not_in_db_and_add(new_url, next_url)

        try:
            html = response.content.decode('utf-8')
        except Exception:
            pass
        finally:
            soup = BeautifulSoup(html, 'html.parser')

            for link in soup.find_all('a'):
                link = str(link.get('href'))
                if link.startswith('http'):
                    urls_list.append(link)
                    print(link)
                elif link.startswith('#'):
                    continue
                elif link is not None and not link.startswith('#'):
                    new_link = given_url + link
                    urls_list.append(new_link)
                    print(new_link)


if __name__ == '__main__':
    arg = sys.argv[1]
    main(arg)
