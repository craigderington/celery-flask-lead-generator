#!/bin/bash

# display earl header
cat earl.txt

# display application description
printf "EARL Distributed Asynchronous Task Queue.  A Python, Flask, Celery, SQLAlchemy & RabbitMQ Joint\n\n"

# the path to the application directory
application_directory="/home/ubuntu/EARL-Automation-Application"

# move to the application directory
echo "Moving to application directory $application_directory"
cd "$application_directory"

# activate the python virtual environment
echo "Activating the python virtual environment, please wait..."
source "/home/ubuntu/EARL-Automation-Application/.env/bin/activate"

# start EARL Automation
printf "Starting EARL automation in $application_directory on $(date)\n\n"
screen -dmS earl-celery-beat celery -A celery_worker:celery beat --loglevel=INFO
screen -dmS get-visitors celery -A celery_worker:celery worker -E -l INFO -n worker0@%h -Q get_visitors -c 1
screen -dmS append-visitors celery -A celery_worker:celery worker -E -l INFO -n worker1@%h -Q append_visitors -c 2
screen -dmS create-leads celery -A celery_worker:celery worker -E -l INFO -n worker2@%h -Q create_leads -c 2
screen -dmS verify-leads celery -A celery_worker:celery worker -E -l INFO -n worker3@%h -Q verify_leads -c 2
screen -dmS send-leads celery -A celery_worker:celery worker -E -l INFO -n worker4@%h -Q send_leads -c 2
screen -dmS send-adfs celery -A celery_worker:celery worker -E -l INFO -n worker5@%h -Q send_adfs -c 2
screen -dmS send-followups celery -A celery_worker:celery worker -E -l INFO -n worker6@%h -Q send_followups -c 2
screen -dmS send-rvms celery -A celery_worker:celery worker -E -l INFO -n worker7@%h -Q send_rvms -c 2
screen -dmS reports-queue celery -A celery_worker:celery worker -E -l INFO -n worker8@%h -Q reports -c 5
screen -dmS campaign-dashboards celery -A celery_worker:celery worker -E -l INFO -n worker9@%h -Q campaigns -c 10
screen -dmS store-dashboards celery -A celery_worker:celery worker -E -l INFO -n worker10@%h -Q stores -c 10

# list the EARL automation queues in screen
echo "EARL Automation started..."
screen -ls


