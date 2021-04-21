mysql -u root -e "CREATE DATABASE IF NOT EXISTS example"
optuna create-study --study-name "distributed-example" --storage "mysql://root@localhost/example"
export NEPTUNE_CUSTOM_RUN_ID=$(uuidgen)