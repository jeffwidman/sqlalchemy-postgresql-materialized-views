# models.py example for use with Flask-SQLAlchemy

from sqlalchemy.ext.hybrid import hybrid_property

from app import db
from app.models.view_factory import MaterializedView, create_mat_view


class GearItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, nullable=False, index=True)
    description = db.Column(db.Text)
    reviews = db.relationship('GearReview', backref='gear_item', lazy='dynamic')
    mat_view = db.relationship('GearItemMV', backref='original',
                            uselist=False, # makes it a one-to-one relationship
                            primaryjoin='GearItem.id==GearItemMV.id',
                            foreign_keys='GearItemMV.id')
    time_created = db.Column(db.DateTime(timezone=True),
            server_default=db.func.current_timestamp(),)

    @hybrid_property
    def review_count(self):
        if self.mat_view is not None: # if None, mat_view needs refreshing
            return self.mat_view.review_count

    @hybrid_property
    def review_rating(self):
        if self.mat_view is not None: # if None, mat_view needs refreshing
            return self.mat_view.review_rating



class GearItemMV(MaterializedView):
    __table__ = create_mat_view("gear_item_mv",
                    db.select(
                        [GearItem.id.label('id'),
                        db.func.count(GearReview.id).label('review_count'),
                        db.func.avg(GearReview.rating).label('review_rating'),]
                    ).select_from(db.join(GearItem, GearReview, isouter=True)
                    ).group_by(GearItem.id))

db.Index('gear_item_mv_id_idx', GearItemMV.id, unique=True)
