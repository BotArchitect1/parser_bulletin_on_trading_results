from sqlalchemy import Column, Integer, String, Float, Date, DateTime
import datetime

from app.db.database import Base


class TradeResult(Base):
    __tablename__ = "spimex_trading_results"
    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String, index=True)
    exchange_product_name = Column(String)
    oil_id = Column(String)
    delivery_basis_id = Column(String)
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String)
    volume = Column(Float)
    total = Column(Float)
    count = Column(Integer)
    date = Column(DateTime)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    updated_on = Column(
        DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )

    def __repr__(self):
        return (
            f"<TradeResult(id={self.id}, "
            f"exchange_product_id={self.exchange_product_id}, "
            f"exchange_product_name={self.exchange_product_name}, "
            f"oil_id={self.oil_id}, "
            f"delivery_basis_id={self.delivery_basis_id}, "
            f"delivery_basis_name={self.delivery_basis_name}, "
            f"delivery_type_id={self.delivery_type_id}, "
            f"volume={self.volume}, total={self.total}, "
            f"count={self.count}, "
            f"date={self.date}, "
            f"created_on={self.created_on}, "
            f"updated_on={self.updated_on})>"
        )
