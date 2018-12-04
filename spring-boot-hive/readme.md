创建Hive表 
待项目启动后，在浏览器中访问 http://localhost:8080/hive2/table/create 来创建一张 user_sample 测试表：

user_sample 表的创建 sql 如下：

    create table user_sample
    ( 
        user_num bigint, 
        user_name string, 
        user_gender string, 
        user_age int
    ) row format delimited fields terminated by ',';
查看Hive表  
测试表创建完成后，通过访问 http://localhost:8080/hive/table/list 来查看hive库中的数据表都有哪些？

返回内容：



在Hive客户端中使用 show tables 命令查看，与浏览器中看到的数据表相同，返回内容：



访问 http://localhost:8080/hive/table/describe?tableName=user_sample 来查看 user_sample 表的字段信息：

返回内容：



 在Hive客户端中使用 describe user_sample 命令进行查看，与浏览器中看到的数据表字段相同。



导入数据 
接下来进行数据导入测试，先在Hive服务器的 /home/hadoop/ 目录下新建一个user_sample.txt 文件，内容如下：

622,Lee,M,25
633,Andy,F,27
644,Chow,M,25
655,Grace,F,24
666,Lily,F,29
677,Angle,F,23
 然后在浏览器中访问以下地址，将 /home/hadoop/user_sample.txt 文件中的内容加载到 user_sample 数据表中。

http://localhost:8080/hive2/table/load

数据导入成功之后，访问 http://localhost:8080/hive/table/select?tableName=user_sample ，返回内容：



 在Hive客户端中使用 select * form user_sample 命令进行查看，与浏览器中看到的内容相同。



插入数据 
再访问  http://localhost:8080/hive2/table/insert 来测试向 user_sample 表中插入一条数据。



Hive客户端打印的Map-Reduce执行过程日志如下：



再次访问 http://localhost:8080/hive/table/select?tableName=user_sample ，返回内容：
