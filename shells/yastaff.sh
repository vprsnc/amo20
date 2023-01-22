#!/nix/store/j4nl4k97148p6kdkh2g2rvb3ab8847vf-python3-3.7.12-env/bin/python #TODO
# echo "hello world!"
cd /home/georgy/Projects/python/amo20
python yastaff_etl_leads.py
python yastaff_etl_calls.py
python yastaff_etl_statuses.py
python yastaff_etl_users.py
python yastaff_etl_pipelines.py
