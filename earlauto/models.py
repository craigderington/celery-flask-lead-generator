from earlauto import db
from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Text, Float
from sqlalchemy.orm import relationship


# Define application db.Models

class User(db.Model):
    __tablename__ = 'ab_user'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(64), nullable=False)
    last_name = Column(String(64), nullable=False)
    username = Column(String(64), unique=True, nullable=False, index=True)
    password = Column(String(256), nullable=False)
    active = Column(Boolean, default=1)
    email = Column(String(120), unique=True, nullable=False)
    last_login = Column(DateTime)
    login_count = Column(Integer)
    fail_login_count = Column(Integer)
    created_on = Column(DateTime, default=datetime.now, nullable=True)
    changed_on = Column(DateTime, default=datetime.now, nullable=True)
    created_by_fk = Column(Integer)
    changed_by_fk = Column(Integer)

    def __repr__(self):
        if self.last_name and self.first_name:
            return '{} {}'.format(
                self.first_name,
                self.last_name
            )


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
    auto_trans_date = Column(String(50))
    last_seen = Column(String(50))
    birth_year = Column(Integer)
    income_range = Column(String(50))
    home_owner_renter = Column(String(50))
    auto_purchase_type = Column(String(100))

    def __repr__(self):
        return '{} {}'.format(
            self.first_name,
            self.last_name
        )
    
    def get_visitor_age(self):
        if self.birth_year:
            today = datetime.now()
            age = int(today.year - self.birth_year)
            return '{}'.format(str(age))
        return ''    


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
    followup_email_sent_date = Column(DateTime)
    followup_email_receipt_id = Column(String(255), nullable=True, default='NOID')
    followup_email_status = Column(String(50), nullable=True, default='NOTSENT')
    followup_email_delivered = Column(Boolean, default=False, nullable=True)
    followup_email_bounced = Column(Boolean, default=False, nullable=True)
    followup_email_opens = Column(Integer, default=0, nullable=True)
    followup_email_clicks = Column(Integer, default=0, nullable=True)
    followup_email_spam = Column(Boolean, default=False, nullable=True)
    followup_email_unsub = Column(Boolean, default=False, nullable=True)
    followup_email_dropped = Column(Boolean, default=False, nullable=True)
    dropped_reason = Column(String(50))
    dropped_code = Column(String(50))
    dropped_description = Column(String(255))
    bounce_error = Column(String(255))
    followup_email_click_ip = Column(String(255))
    followup_email_click_device = Column(String(255))
    followup_email_click_campaign = Column(String(255))
    webhook_last_update = Column(DateTime, onupdate=datetime.now, nullable=True)
    followup_email_open_campaign = Column(String(255), nullable=True)
    followup_email_open_ip = Column(String(255), nullable=True)
    followup_email_open_device = Column(String(255), nullable=True)

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
    archived = Column(Boolean(), default=0)
    archived_by = Column(String(50), nullable=True)
    archived_date = Column(DateTime, nullable=True)

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
    creative_header = Column(Text)
    creative_footer = Column(Text)
    email_subject = Column(String(255))
    rvm_campaign_id = Column(Integer, unique=True, nullable=True, default=0)
    rvm_send_count = Column(Integer, default=0)
    rvm_limit = Column(Integer, nullable=False, default=10000)
    adf_subject = Column(String(255))
    archived = Column(Boolean(), default=0)
    archived_by = Column(String(50), nullable=True)
    archived_date = Column(DateTime, nullable=True)
    send_dealer = Column(Boolean(), default=0)
    send_adf = Column(Boolean(), default=0)
    send_email = Column(Boolean(), default=0)
    send_rvm = Column(Boolean(), default=0)

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


class Contact(db.Model):
    __tablename__ = 'contacts'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('stores.id'))
    first_name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    title = Column(String(50), nullable=True)
    email = Column(String(255), unique=True, nullable=False)
    mobile = Column(String(255), unique=True, nullable=False)


class GlobalDashboard(db.Model):
    __tablename__ = 'dashboard'
    id = Column(Integer, primary_key=True)
    total_stores = Column(Integer, default=0, nullable=False)
    active_stores = Column(Integer, default=0, nullable=False)
    total_campaigns = Column(Integer, default=0, nullable=False)
    active_campaigns = Column(Integer, default=0, nullable=False)
    total_global_visitors = Column(Integer, default=0, nullable=False)
    total_unique_visitors = Column(Integer, default=0, nullable=False)
    total_us_visitors = Column(Integer, default=0, nullable=False)
    total_appends = Column(Integer, default=0, nullable=False)
    total_sent_to_dealer = Column(Integer, default=0, nullable=False)
    total_sent_followup_emails = Column(Integer, default=0, nullable=False)
    total_rvms_sent = Column(Integer, default=0, nullable=False)
    global_append_rate = Column(Float, default=0.00, nullable=False)
    unique_append_rate = Column(Float, default=0.00, nullable=False)
    us_append_rate = Column(Float, default=0.00, nullable=False)
    last_update = Column(DateTime, onupdate=datetime.now, nullable=True)

    def __repr__(self):
        return '{}'.format(self.id)


class StoreDashboard(db.Model):
    __tablename__ = 'store_dashboard'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('stores.id'))
    store_name = relationship("Store")
    total_campaigns = Column(Integer, default=0, nullable=False)
    active_campaigns = Column(Integer, default=0, nullable=False)
    total_global_visitors = Column(Integer, default=0, nullable=False)
    total_unique_visitors = Column(Integer, default=0, nullable=False)
    total_us_visitors = Column(Integer, default=0, nullable=False)
    total_appends = Column(Integer, default=0, nullable=False)
    total_sent_to_dealer = Column(Integer, default=0, nullable=False)
    total_sent_followup_emails = Column(Integer, default=0, nullable=False)
    total_rvms_sent = Column(Integer, default=0, nullable=False)
    global_append_rate = Column(Float, default=0.00, nullable=False)
    unique_append_rate = Column(Float, default=0.00, nullable=False)
    us_append_rate = Column(Float, default=0.00, nullable=False)
    last_update = Column(DateTime, onupdate=datetime.now, nullable=True)

    def __repr__(self):
        return '{}'.format(self.id)


class CampaignDashboard(db.Model):
    __tablename__ = 'campaign_dashboard'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('stores.id'), nullable=False)
    store_name = relationship("Store")
    campaign_id = Column(Integer, ForeignKey('campaigns.id'), nullable=False)
    campaign_name = relationship("Campaign")
    total_visitors = Column(Integer, default=0, nullable=True)
    total_appends = Column(Integer, default=0, nullable=True)
    total_rtns = Column(Integer, default=0, nullable=True)
    total_followup_emails = Column(Integer, default=0, nullable=True)
    total_rvms = Column(Integer, default=0, nullable=True)
    append_rate = Column(Float, default=0.00, nullable=True)
    last_update = Column(DateTime, nullable=True)
    global_visitors = Column(Integer, default=0, nullable=True)
    unique_visitors = Column(Integer, default=0, nullable=True)

    def __repr__(self):
        return '{} {} {}'.format(
            self.store_name,
            self.campaign_name,
            str(self.last_update)
        )
