# materialized_view_factory.py
# standalone SQLAlchemy example

# Accompanying blog post: 
# http://www.jeffwidman.com/blog/847/

# Many thanks to Mike Bayer (@zzzeek) for his help.

from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
import sqlalchemy as db


class CreateMaterializedView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


@compiler.compiles(CreateMaterializedView)
def compile(element, compiler, **kw):
    # Could use "CREATE OR REPLACE MATERIALIZED VIEW..."
    # but I'd rather have noisy errors
    return "CREATE MATERIALIZED VIEW %s AS %s" % (
        element.name,
        compiler.sql_compiler.process(element.selectable),
        )


def create_mat_view(metadata, name, selectable):
    _mt = db.MetaData() # temp metadata just for initial Table object creation
    t = db.Table(name, _mt) # the actual mat view class is bound to db.metadata
    for c in selectable.c:
        t.append_column(db.Column(c.name, c.type, primary_key=c.primary_key))

    db.event.listen(
        metadata, "after_create",
        CreateMaterializedView(name, selectable)
    )

    @db.event.listens_for(metadata, "after_create")
    def create_indexes(target, connection, **kw):
        for idx in t.indexes:
            idx.create(connection)

    db.event.listen(
        metadata, "before_drop",
        db.DDL('DROP MATERIALIZED VIEW IF EXISTS ' + name)
    )
    return t


from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class Gear(Base):
    __tablename__ = 'gear'
    id = db.Column(db.Integer, primary_key=True)
    rating = db.Column(db.Integer)


class GearMV(Base):
    __table__ = create_mat_view(
        Base.metadata,
        "gear_mv",
        db.select(
            [Gear.id.label('id'),
             db.func.count(Gear.id).label('review_count'),
             db.func.avg(Gear.rating).label('review_rating'),]
        ).select_from(Gear)
        .group_by(Gear.id))

db.Index('test_index', GearMV.id, unique=True)


e = db.create_engine("postgresql://scott:tiger@localhost/test", echo=True)
Base.metadata.drop_all(e)
Base.metadata.create_all(e)

Base.metadata.drop_all(e)
