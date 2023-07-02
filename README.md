# Проект переноса данных из GP - ClickHouse
Проект предназначен для переноса SQL запроса из GP в ClickHouse.

# Архитектура прокта
Проект содержит модули:
- Airflow - автоматической генерации DAG для airflow


# Запуск проекта
Подробнее в видео, которое посвященно обзору решения [Видео "Перенос витрин из GP в ClickHouse" Youtube](https://youtu.be/dey55MKv6-U)

## Применение пользователя
Проетк собран в docker-compose который и обеспечивает развертывание кластера. Для его запуска требуется в файл .env загрузить идентификатор текущего пользователя, для нормального чтения airflow кода исполнения, который находится в папке dags/
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
## Пересборка docker образа для worker
Для чего нужна пересборка образа для worker с целью показать возможности:
- worker может содержать спецефические пакеты к примеру yoyo, которых нет в исходном образе.
- worker может содержать  папки, для обеспечения работоспособности решения.
Тем самым мы можем автоматизировать сборку необходимого образа для развертывания CI/CD в том же самом kuber для исполнения наших процессов. Можно было использовать и виртуальные операторы для испольнения спецефичного кода, но так на мой взгляд будет точнее. 
```sh
docker-compose build
```

## Запуск проекта
После пересборки проекта, приступаем к запуску кластера для проведения тестов:
```sh
docker-compose up -d
```
Тестирование:
- создать подключение gp_admin, которое необходимо для работы с GP. Доступы см. ниже.
- создание s3 бакета (dp-transfer-sql) на minio. Параметр берется из переменной Airflow transfer_to_click["S3_BUCKET"] [var.json](./var.json)
- создание таблиц необходимых для проведения тестов находится в файле  [test_sql/test_sql.sql](./test_sql/test_sql.sql)
- Завести pool для переноса данных "transfer_pool"
- запуск DAG нарезки переноса по написанной  [yaml](./dags/transfersql_configs/transfer_sql.yaml) схеме.
Более подробнее на видео.

## Доступы
Доступы все можно увидеть в файле  [docker-compose.yaml](./docker-compose.yaml)
- Airflow - host: localhost:8080 login: airflow password: airflow
- Greenplum - host: localhost (из airflow gpdb) port: 5432, db: greenplum, login test, password: test.
- Minio - host: localhost:9001 login: minio, password: minio123 (пароль и логин заведен в переменные окружения worker)
- Доступ из Greenplum в Minio расположен в файле config/minio-site.xml
- ClickHouse 23.4.2. без проверки доступа


## Остановка проекта
После проведения тестов останавливаем проект через комманду:
```sh
docker-compose down
```

## Ссылки
- [Видео "Greenplum хранение таблиц" Youtube](https://youtu.be/yV0leI-lRWM)
- [Видео "Greenplum PXF S3" Youtube](https://youtu.be/iz-J_yFHgTE)
- [Библиотека миграций для ClickHouse](https://github.com/Infinidat/infi.clickhouse_orm)