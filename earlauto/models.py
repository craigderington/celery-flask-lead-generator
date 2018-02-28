from earlauto import db
from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Text
from sqlalchemy.orm import relationship


# Define application db.Models


class Visitor(db.Model):
    __tablename__ = 'visitors'
    id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer, ForeignKey('campaigns.id'), nullable=False)
    store_id = Column(Integer, ForeignKey('stores.id'))
    created_date = Column(DateTime, onupdate=datetime.now)
    ip = Column(String(15), index=True)
    user_agent = Column(String(255))
    job_number = Column(Integer)
    client_id = Column(String(255))
    appended = Column(Boolean, default=False)
    open_hash = Column(String(255))
    campaign_hash = Column(String(255))
    send_hash = Column(String(255))
    num_visits = Column(Integer)
    last_visit = Column(DateTime)
    raw_data = Column(Text)
    processed = Column(Boolean, default=False)
    campaign = relationship("Campaign")
    store = relationship("Store")
    country_name = Column(String(255))
    city = Column(String(255))
    time_zone = Column(String(50))
    longitude = Column(String(50))
    latitude = Column(String(50))
    metro_code = Column(String(10))
    country_code = Column(String(2))
    country_code3 = Column(String(3))
    dma_code = Column(String(3))
    area_code = Column(String(3))
    postal_code = Column(String(5))
    region = Column(String(50))
    region_name = Column(String(255))
    traffic_type = Column(String(255))
    retry_counter = Column(Integer)
    last_retry = Column(DateTime)
    status = Column(String(10))
    locked = Column(Boolean, default=0)

    def __repr__(self):
        return 'From {} on {} for {}'.format(
            self.ip,
            self.created_date,
            self.campaign
        )

    def get_geoip_data(self):
        return '{} {} {} {} {}'.format(
            self.country_code,
            self.city,
            self.region,
            self.postal_code,
            self.traffic_type
        )


class AppendedVisitor(db.Model):
    __tablename__ = 'appendedvisitors'
    id = Column(Integer, primary_key=True)
    visitor = Column(Integer, ForeignKey('visitors.id'))
    visitor_relation = relationship("Visitor")
    created_date = Column(DateTime, onupdate=datetime.now)
    first_name = Column(String(255))
    last_name = Column(String(255))
    email = Column(String(255))
    home_phone = Column(String(15))
    cell_phone = Column(String(15))
    address1 = Column(String(255))
    address2 = Column(String(255))
    city = Column(String(255))
    state = Column(String(2))
    zip_code = Column(String(5))
    zip_4 = Column(Integer)
    credit_range = Column(String(50))
    car_year = Column(String(10))
    car_make = Column(String(255))
    car_model = Column(String(255))
    processed = Column(Boolean, default=False)
    ppm_type = Column(String(10))
    ppm_indicator = Column(String(10))
    ppm_segment = Column(String(50))

    def __repr__(self):
        return '{} {}'.format(
            self.first_name,
            self.last_name
        )


class Lead(db.Model):
    __tablename__ = 'leads'
    id = Column(Integer, primary_key=True)
    appended_visitor_id = Column(Integer, ForeignKey('appendedvisitors.id'), nullable=False)
    appended_visitor = relationship("AppendedVisitor")
    created_date = Column(DateTime, onupdate=datetime.now)
    email_verified = Column(Boolean, default=False)
    lead_optout = Column(Boolean, default=False)
    processed = Column(Boolean, default=False)
    followup_email = Column(Boolean, default=False)
    followup_voicemail = Column(Boolean, default=False)
    followup_print = Column(Boolean, default=False)
    email_receipt_id = Column(String(255))
    sent_to_dealer = Column(Boolean, default=False)
    email_validation_message = Column(String(50))
    sent_adf = Column(Boolean, default=False)
    adf_email_receipt_id = Column(String(255))
    adf_email_validation_message = Column(String(50))
    rvm_status = Column(String(20), nullable=True)
    rvm_date = Column(DateTime)
    rvm_message = Column(String(50))
    rvm_sent = Column(Boolean, default=0, nullable=False)

    def __repr__(self):
        return '{}'.format(
            self.id
        )


class Store(db.Model):
    __tablename__ = 'stores'
    id = Column(Integer, primary_key=True)
    client_id = Column(String(255), unique=True)
    name = Column(String(255), unique=True)
    address1 = Column(String(255))
    address2 = Column(String(255))
    city = Column(String(255))
    state = Column(String(2))
    zip_code = Column(Integer)
    zip_4 = Column(Integer)
    status = Column(String(20), default='Active')
    adf_email = Column(String(255))
    notification_email = Column(String(255))
    reporting_email = Column(String(255))
    phone_number = Column(String(20))
    simplifi_company_id = Column(Integer)
    simplifi_client_id = Column(String(255))
    simplifi_name = Column(String(255))

    def __repr__(self):
        return '{}'.format(
            self.name
        )


class CampaignType(db.Model):
    __tablename__ = 'campaigntypes'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))

    def __repr__(self):
        return '{}'.format(
            self.name
        )


class Campaign(db.Model):
    __tablename__ = 'campaigns'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('stores.id'))
    store = relationship("Store")
    name = Column(String(255))
    job_number = Column(Integer, unique=True)
    created_date = Column(DateTime, onupdate=datetime.now)
    created_by = Column(Integer, ForeignKey('ab_user.id'))
    type = Column(Integer, ForeignKey('campaigntypes.id'))
    campaign_type = relationship("CampaignType")
    options = Column(Text)
    description = Column(Text)
    funded = Column(Boolean, default=0)
    approved = Column(Boolean, default=0)
    approved_by = Column(Integer, ForeignKey('ab_user.id'))
    status = Column(String(255))
    objective = Column(Text)
    frequency = Column(String(255))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    radius = Column(Integer, default=50)
    pixeltrackers_id = Column(Integer, ForeignKey('pixeltrackers.id'))
    pixeltracker = relationship("PixelTracker")
    client_id = Column(String(20))

    def __repr__(self):
        return '{}'.format(
            self.name
        )


class PixelTracker(db.Model):
    __tablename__ = 'pixeltrackers'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)
    ip_addr = Column(String(15), unique=True)
    fqdn = Column(String(255), unique=True)
    capacity = Column(Integer, default=200)
    total_campaigns = Column(Integer)
    active = Column(Boolean, default=1)

    def __repr__(self):
        return '{}'.format(
            self.name
        )
