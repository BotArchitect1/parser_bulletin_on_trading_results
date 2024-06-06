from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import async_sessionmaker

from sqlalchemy.engine import URL

from app.config import config

DATABASE_URL: URL = URL.create(
    drivername="postgresql+asyncpg",
    username=config.db.user,
    password=config.db.password,
    host=config.db.host,
    port=config.db.port,
    database=config.db.name,
)

Base = declarative_base()

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
async_session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


