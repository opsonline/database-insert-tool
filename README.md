# import_data_to_mysql

脚本说明
--------
这是一个将本地csv/excel文件数据插入到MySQL数据库脚本工具

参数说明：
---------
```
-h, --help      show this help message and exit
-H, --host      mysql主机地址，默认：127.0.0.1
-P, --port      mysql端口地址，默认：3306
-u, --user      mysql连接用户名, 默认：root
-p, --password  mysql连接用户名的密码, 默认：123456
-d, --db        数据库名
-t, --table     表名
-f, --file      本地文件路径
-e, --encoding  文件的编码格式，默认：utf-8
```

安装依赖包
--------
```
pip3 install -r requirements.txt
```

使用示例：
--------
```
python3 import_data_to_mysql.py --host 127.0.0.1 --db test --table t1 --user user_admin --file
/mnt/c/Users/kehongping/Desktop/xls/test.csv --encoding gbk
```
**注意：** excel/csv文件中的列名必须要和数据库表的字段名一样，要插入的数据库必须是已经存在的数据库和数据表，若没有需要先手动创建