"""
Database entities (models) defined using SQLAlchemy ORM and DDL.
"""

from sqlalchemy import (
    DDL,
    Boolean,
    Column,
    Float,
    ForeignKey,
    SmallInteger,
    String,
    event,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..utils import read_data_csv


class Base(DeclarativeBase):
    """
    Declarative base class for registering tables.

    See https://docs.sqlalchemy.org/en/20/orm/mapping_api.html#sqlalchemy.orm.DeclarativeBase.
    """

    pass


class Country(Base):
    """
    Country table based on the UN M49 methodology.

    See https://unstats.un.org/unsd/methodology/m49/.
    """

    __tablename__ = "country"

    id: Mapped[int] = mapped_column(primary_key=True)
    iso_2: Mapped[str] = mapped_column(String(2), nullable=False, unique=True)
    iso_3: Mapped[str] = mapped_column(String(3), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    subregion: Mapped[str] = mapped_column(String(64), nullable=False)
    region: Mapped[str] = mapped_column(String(64), nullable=False)
    ldc: Mapped[bool] = mapped_column(Boolean, nullable=False)
    lldc: Mapped[bool] = mapped_column(Boolean, nullable=False)
    sids: Mapped[bool] = mapped_column(Boolean, nullable=False)


class Indicator(Base):
    """
    Indicator table.
    """

    __tablename__ = "indicator"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(1024), nullable=False, unique=True)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)


class Dimension(Base):
    """
    Disaggregation table.

    For simplicity and consistency across sources, all disaggregations are stored as a single
    unique `name` field.
    """

    __tablename__ = "dimension"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(1024), nullable=False, unique=True)


class Series(Base):
    """
    Series table.

    A series is defined as a time series for a single country, indicator and disaggregation.
    """

    __tablename__ = "series"

    country_id: Mapped[int] = mapped_column(
        ForeignKey("country.id", ondelete="CASCADE"), primary_key=True
    )
    indicator_id: Mapped[int] = mapped_column(
        ForeignKey("indicator.id", ondelete="CASCADE"), primary_key=True
    )
    dimension_id: Mapped[int] = mapped_column(
        ForeignKey("dimension.id", ondelete="CASCADE"), primary_key=True
    )
    year: Mapped[int] = Column(SmallInteger, primary_key=True)
    value: Mapped[float] = Column(Float(32))


@event.listens_for(Base.metadata, "after_create")
def create_view(target, connection, **kwargs):
    """
    Create a queryable observation view upon table creation.
    """
    connection.execute(
        DDL(
            """
            CREATE VIEW observation AS
                SELECT
                    c.id AS country_id,
                    iso_2 AS country_code_2,
                    iso_3 AS country_code_3,
                    c.name AS country_name,
                    i.id AS indicator_id,
                    i.name AS indicator_name,
                    i.provider AS indicator_provider,
                    d.id AS dimension_id,
                    d.name AS dimension_name,
                    year,
                    value,
                    subregion,
                    region,
                    ldc,
                    lldc,
                    sids
                FROM
                    series AS s
                LEFT OUTER JOIN country AS c ON s.country_id = c.id
                LEFT OUTER JOIN indicator AS i ON s.indicator_id = i.id
                LEFT OUTER JOIN dimension AS d ON s.dimension_id = d.id
            ;
            """
        )
    )


@event.listens_for(Base.metadata, "after_create")
def insert_country_data(target, connection, **kwargs):
    """
    Insert M49 data into the country table upon table creation.

    See https://unstats.un.org/unsd/methodology/m49/.
    """
    columns = {
        "M49 Code": "id",
        "ISO-alpha2 Code": "iso_2",
        "ISO-alpha3 Code": "iso_3",
        "Country or Area": "name",
        "Region Name": "region",
        "Sub-region Name": "subregion",
        "Least Developed Countries (LDC)": "ldc",
        "Land Locked Developing Countries (LLDC)": "lldc",
        "Small Island Developing States (SIDS)": "sids",
    }
    df = read_data_csv("unsd-m49.csv", sep=";", keep_default_na=False)
    df = df.reindex(columns=columns).rename(columns=columns)
    for column in ("ldc", "lldc", "sids"):
        df[column] = df[column].eq("x")
    df.sort_values("id", ignore_index=True, inplace=True)
    df.to_sql(
        "country", con=connection, if_exists="replace", index=False, method="multi"
    )
