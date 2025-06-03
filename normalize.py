import pandas as pd
import subprocess
import os


input_file_path = "C:\\Users\\litoh\\Desktop\\3HDFS\\World Important Dates.csv"
output_file_path = "C:\\Users\\litoh\\Desktop\\3HDFS\\out_data.parquet"
hdfs_path = '/user/out_data.parquet'
master_container_id = 'namenode'



df = pd.read_csv(input_file_path, encoding='utf-8')

df.dropna(inplace=True)
df.drop_duplicates(inplace=True)

df.to_parquet(output_file_path, index=False)
docker_cp_command = f'docker cp {output_file_path} {master_container_id}:/tmp/out_data.parquet'
subprocess.run(docker_cp_command, shell=True)
# Создаём директорию /user в HDFS, если её нет
mkdir_command = f'docker exec {master_container_id} hdfs dfs -mkdir -p /user'
subprocess.run(mkdir_command, shell=True)
hdfs_command = f'docker exec {master_container_id} hdfs dfs -put /tmp/out_data.parquet {hdfs_path}'
subprocess.run(hdfs_command, shell=True)
check_command = f'docker exec {master_container_id} hdfs dfs -ls {hdfs_path}'
result = subprocess.run(check_command, shell=True, capture_output=True, text=True)
print(result.stdout)
