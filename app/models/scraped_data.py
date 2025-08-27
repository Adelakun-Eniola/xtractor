from app import db
from datetime import datetime

class ScrapedData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_name = db.Column(db.String(200), nullable=True)
    email = db.Column(db.String(120), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    address = db.Column(db.Text, nullable=True)
    website_url = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Foreign key to user
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    
    def __repr__(self):
        return f'<ScrapedData {self.company_name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'company_name': self.company_name,
            'email': self.email,
            'phone': self.phone,
            'address': self.address,
            'website_url': self.website_url,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S')
        }