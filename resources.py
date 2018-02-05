from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(20), nullable=False)
    password = Column(String(100), nullable=False)
    email = Column(String(80), nullable=False)

    def to_dict(self):
        return {'id': self.id, 'username': self.username, 'email': self.email}

    def __repr__(self):
        return "<User(name={})>".format(self.username)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        getattr(self, key)
        return setattr(self, key, value)


class Comment(Base):
    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    text = Column(String(300), nullable=False)

    user = relationship("User", back_populates="comments")

    def __repr__(self):
        return "<Comment(text={})>".format(self.text)

User.comments = relationship('Comment', order_by=Comment.id, back_populates="user")
